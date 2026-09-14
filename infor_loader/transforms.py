from __future__ import annotations

import logging
import re
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd


Transform = Callable[[pd.DataFrame, Any, dict[str, pd.DataFrame], logging.Logger], pd.DataFrame]
TRANSFORMS: dict[str, Transform] = {}

# Non-fatal data-quality notes raised by a transform -- e.g. a malformed source
# value it had to repair. FileLoader._prepare_dataframe clears this before the
# transform chain and drains it afterwards onto the ETLHealth staging row, so a
# silent repair is still visible on the health report. Held outside the transform
# signature (which is fixed across every transform) and kept thread-local, because
# the CLI can run loaders in parallel (run_loaders' ThreadPoolExecutor) and each
# run's notes must stay on its own health row.
_TRANSFORM_STATE = threading.local()


def _transform_warnings() -> list[str]:
    warnings = getattr(_TRANSFORM_STATE, "warnings", None)
    if warnings is None:
        warnings = []
        _TRANSFORM_STATE.warnings = warnings
    return warnings


def record_transform_warning(message: str) -> None:
    """Surface ``message`` on this run's ETLHealth row as a non-fatal warning."""
    _transform_warnings().append(message)


def drain_transform_warnings() -> list[str]:
    """Return and clear the warnings recorded on this thread since the last drain."""
    warnings = _transform_warnings()
    drained = list(warnings)
    warnings.clear()
    return drained


def register(name: str) -> Callable[[Transform], Transform]:
    def decorator(func: Transform) -> Transform:
        TRANSFORMS[name] = func
        return func

    return decorator


Q_MAP = {
    "01": "Q1",
    "02": "Q1",
    "03": "Q1",
    "04": "Q2",
    "05": "Q2",
    "06": "Q2",
    "07": "Q3",
    "08": "Q3",
    "09": "Q3",
    "10": "Q4",
    "11": "Q4",
    "12": "Q4",
}


def _parse_datetime(series: pd.Series, fmt: str | None) -> pd.Series:
    """Parse a string column to datetime.

    An explicit ``fmt`` (e.g. ``%m/%d/%Y %I:%M:%S %p``) is fastest and
    unambiguous. Without one, parse as ``mixed`` so pandas does not emit the
    "Could not infer format ... falling back to dateutil" warning and silently
    parse every value individually.
    """
    return pd.to_datetime(series, format=fmt or "mixed", errors="coerce")


def apply_type_changes(
    df: pd.DataFrame,
    type_changes: dict[str, list[str]],
    *,
    date_format: str | None = None,
    datetime_format: str | None = None,
    datetime_to_date: list[str] | None = None,
) -> pd.DataFrame:
    # Columns parsed with the datetime format but destined for a DATE column
    # must be truncated here: with fast_executemany the ODBC driver casts each
    # parameter to the described column type on the client, and a non-midnight
    # timestamp -> DATE cast raises 22008 "Datetime field overflow" instead of
    # truncating the way SQL Server would server-side.
    truncate_to_date = set(datetime_to_date or [])
    for col in type_changes.get("date", []):
        if col in df.columns:
            converted = _parse_datetime(df[col], date_format)
            df[col] = converted.fillna(pd.to_datetime("1900-01-01")).dt.date
    for col in type_changes.get("datetime", []):
        if col in df.columns:
            converted = _parse_datetime(df[col], datetime_format)
            converted = converted.fillna(pd.to_datetime("1900-01-01"))
            df[col] = converted.dt.date if col in truncate_to_date else converted
    for col in type_changes.get("float", []):
        if col in df.columns:
            df[col] = df[col].apply(_to_float)
    for col in type_changes.get("int", []):
        if col in df.columns:
            df[col] = df[col].apply(_to_int)
    return df


def normalize_for_db(
    df: pd.DataFrame,
    type_changes: dict[str, list[str]],
    *,
    preserve_whitespace: list[str] | None = None,
) -> pd.DataFrame:
    int_cols = set(type_changes.get("int", []))
    # date/datetime blanks were already filled with 1900-01-01 by apply_type_changes
    # (the single blank-date sentinel; prod was backfilled 0001-01-01 -> 1900-01-01
    # on 2026-07-15), so they pass through untouched here along with floats.
    passthrough = (
        set(type_changes.get("date", []))
        | set(type_changes.get("datetime", []))
        | set(type_changes.get("float", []))
    )
    passthrough.add("report stamp")
    # Columns whose whitespace is significant to the destination (e.g. a PK that
    # relies on leading spaces to keep near-duplicate rows distinct -- see
    # LoaderConfig.preserve_whitespace_columns): blanks are still filled with ''
    # but the values are NOT trimmed.
    keep_whitespace = set(preserve_whitespace or [])

    for col in df.columns:
        if col in int_cols:
            df[col] = df[col].astype(object).where(df[col].notna(), None)
        elif col in passthrough:
            continue
        elif col in keep_whitespace:
            df[col] = df[col].fillna("")
            df[col] = df[col].apply(str)
        else:
            df[col] = df[col].fillna("")
            df[col] = df[col].apply(lambda x: str(x).strip())
    return df


def _fill_string_blanks(df: pd.DataFrame) -> pd.DataFrame:
    """Fill blanks with '' in string (object-dtype) columns only.

    Numeric and datetime columns keep NaN/NaT so blanks land as SQL NULL --
    a blanket ``df.fillna("")`` would stuff '' into float columns, which the
    numeric destination column rejects at insert time. (Date/datetime columns
    typed in the mapping already carry the 1900-01-01 sentinel by the time a
    transform runs, so they pass through unchanged either way.)
    """
    string_columns = [column for column in df.columns if df[column].dtype == object]
    df[string_columns] = df[string_columns].fillna("")
    return df


def add_quarter(year_month: Any) -> Any:
    if pd.isnull(year_month):
        return np.nan
    parts = str(year_month).split("-")
    if len(parts) < 2:
        return np.nan
    return Q_MAP.get(parts[1], np.nan)


def move_files(moves: list[dict[str, Any]], logger: logging.Logger) -> None:
    for move in moves:
        source_path = Path(move["source_path"])
        destination_path = Path(move["destination_path"])
        pattern = move.get("pattern", "*")
        action = move.get("action", "move")
        pick_latest = bool(move.get("pick_latest", False))
        required = bool(move.get("required", True))

        matches = sorted(source_path.glob(pattern), key=lambda p: p.stat().st_mtime)
        if not matches:
            message = f"No files matched {source_path / pattern}"
            if required:
                raise FileNotFoundError(message)
            logger.warning(message)
            continue

        files = [matches[-1]] if pick_latest else matches
        destination_path.mkdir(parents=True, exist_ok=True)
        for file_path in files:
            destination = destination_path / file_path.name
            if action == "copy":
                shutil.copy2(file_path, destination)
            elif action == "move":
                shutil.move(str(file_path), str(destination))
            else:
                raise ValueError(f"Unsupported file move action: {action}")
            logger.info("%s %s -> %s", action, file_path, destination)


@register("dedup_keep_first")
def dedup_keep_first(
    df: pd.DataFrame,
    loader_config: Any,
    dataframes: dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    """Config-driven duplicate removal, replicating a SQL ROW_NUMBER() dedup:

        ROW_NUMBER() OVER (PARTITION BY <keys> ORDER BY <order_by>) = 1

    Driven by ``transform_options.dedup``:
      keys:     partition columns; one output row is kept per distinct key.
      order_by: list of ``{column, direction}`` (direction: asc | desc, default
                asc) ranking the rows within a key; the first row wins.

    Transforms run on *source* column names, before column mapping and the
    pk_check, so a source file that ships exact-key duplicates is reduced to one
    row per key here and pk_check then only guards genuinely unexpected
    collisions. Ties on the full order_by keep their file order (stable sort),
    where SQL ROW_NUMBER ties are arbitrary.
    """
    options = dict(loader_config.transform_options.get("dedup") or {})
    keys = [str(key) for key in options.get("keys") or []]
    if not keys:
        raise ValueError("dedup_keep_first requires transform_options.dedup.keys")
    order_by = [dict(item) for item in options.get("order_by") or []]

    columns_used = [*keys, *(item.get("column") for item in order_by)]
    missing = [column for column in columns_used if column not in df.columns]
    if missing:
        raise ValueError(f"dedup_keep_first columns missing from source: {missing}")

    before = len(df)
    if order_by:
        sort_columns = [item["column"] for item in order_by]
        ascending = [str(item.get("direction", "asc")).strip().lower() != "desc" for item in order_by]
        df = df.sort_values(sort_columns, ascending=ascending, kind="stable")
    # sort_index restores the original file order for the surviving rows.
    df = df.drop_duplicates(subset=keys, keep="first").sort_index()
    logger.info(
        "dedup_keep_first: removed %s duplicate row(s) on keys %s (%s -> %s rows).",
        before - len(df),
        keys,
        before,
        len(df),
    )
    return df


@register("inventory_location_report_stamp")
def inventory_location_report_stamp(
    df: pd.DataFrame,
    loader_config: Any,
    dataframes: dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    df["report stamp"] = df["update stamp"].max()
    return df


@register("inventory_transaction_accounts")
def inventory_transaction_accounts(
    df: pd.DataFrame,
    loader_config: Any,
    dataframes: dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    df["OffsetIndex"] = df["OffsetAccount"].apply(lambda x: _split_pipe(x, 1))
    df["OffsetGL"] = df["OffsetAccount"].apply(lambda x: _split_pipe(x, 2))
    df["InventoryIndex"] = df["InventoryAccount"].apply(lambda x: _split_pipe(x, 1))
    df["InventoryGL"] = df["InventoryAccount"].apply(lambda x: _split_pipe(x, 2))
    return df


@register("purchase_order_line")
def purchase_order_line(
    df: pd.DataFrame,
    loader_config: Any,
    dataframes: dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    """Enrich the PO line export and derive its computed/reporting columns.

    The mapping inputs come from the loader's extra source files, by alias:
      company_map -> company hierarchy (Name, AP pay lv3, AP pay lv1)
      fd3         -> FinanceDimension3 descriptions (FD3Text)
      fd5         -> FinanceDimension5 descriptions (FD5Text)
    """
    options = loader_config.transform_options
    missing_aliases = [alias for alias in ("company_map", "fd3", "fd5") if alias not in dataframes]
    if missing_aliases:
        raise ValueError(
            f"purchase_order_line requires source files with aliases {missing_aliases}; "
            f"got {sorted(dataframes)}"
        )

    company_map = dataframes["company_map"]
    cmap = (
        df[["Company"]]
        .drop_duplicates()
        .merge(
            company_map[["Company", "Name", "AP pay lv3", "AP pay lv1"]]
            .dropna()
            .drop_duplicates(subset=["Company"]),
            on=["Company"],
            how="left",
        )
    )
    missing_company = cmap[cmap[["Name", "AP pay lv3", "AP pay lv1"]].isna().any(axis=1)]
    if not missing_company.empty:
        logger.warning("Unmapped companies: %s", missing_company["Company"].drop_duplicates().tolist())
        if options.get("fail_on_unmapped_company", False):
            raise ValueError("Company map has unmapped companies; see loader log.")
    df = df.merge(cmap, on=["Company"], how="left")

    fd3_m = dataframes["fd3"][["FinanceDimension3", "Description"]].copy()
    fd3_m.columns = ["FD3", "FD3Text"]
    fd5_m = dataframes["fd5"][["FinanceDimension5", "Description"]].copy()
    fd5_m.columns = ["FD5", "FD5Text"]
    df = df.merge(fd3_m, on=["FD3"], how="left").merge(fd5_m, on=["FD5"], how="left")

    inventory_index = df[~df["InventoryFD1"].isnull()].index
    df.loc[inventory_index, "FD5"] = "INVENTORY - " + df.loc[inventory_index, "TransientInventoryLocation"].fillna("")
    df.loc[inventory_index, "FD5Text"] = df.loc[inventory_index, "FD5"]
    df["FD5Text"] = df["FD5Text"].fillna(df["FD1Text"])

    df["PurchaseOrder.Reference1"] = df["PurchaseOrder.Reference1"].apply(lambda x: _remove_leading_zeros(str(x).strip()))

    # A PO transferred from the legacy ERP carries the old order number in
    # Reference1, so any non-empty reference marks the line as transferred. The
    # 10-character company-prefixed form is a subset of "non-empty" and is spelled
    # out only to document the known shape of transferred numbers. This replaces
    # the old reference_check/'Helper' column + manual still_need_list machinery,
    # which had converged to exactly this rule.
    reference = df["PurchaseOrder.Reference1"]
    legacy_mask = (reference != "") | (
        (reference.str.len() == 10) & (reference.str[:4] == df["Company"])
    )
    unreleased_ind = df[
        (df["PurchaseOrder.IsReleased"] == "No")
        & (df["PurchaseOrder.DerivedPurchaseOrderStatus"] != "Closed")
    ].index
    canceled_after_ind = df[
        (df["LineReleased"] == "Yes")
        & (df["PurchaseOrder.DerivedPurchaseOrderStatus"] == "Canceled")
    ].index

    df["EXC_FLAG"] = "Default"
    df.loc[legacy_mask, "EXC_FLAG"] = "Remove - legacy PO transferred"
    df.loc[unreleased_ind, "EXC_FLAG"] = "Remove - unreleased"
    df.loc[canceled_after_ind, "EXC_FLAG"] = "Remove - released but canceled"

    df["System"] = "INFOR"
    # POReleaseDate blanks are already the 1900-01-01 sentinel (apply_type_changes
    # runs before transforms), so a PO without a release date deliberately reports
    # Year 1900 / YearMonth 1900-01 / Quarter Q1 -- the old notebook fallback to
    # the PO date was retired on purpose.
    df["Year"] = df["PurchaseOrder.MMAHSPOReleaseDate"].apply(lambda x: str(x)[:4])
    df["YearMonth"] = df["PurchaseOrder.MMAHSPOReleaseDate"].apply(lambda x: str(x)[:7])
    df["Quarter"] = df["YearMonth"].apply(add_quarter)
    return df


@register("requisition_line_basecost_uom")
def requisition_line_basecost_uom(
    df: pd.DataFrame,
    loader_config: Any,
    dataframes: dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    df["ContractLine.BaseCostUOM"] = df["ContractLine.BaseCostUOM"].apply(
        lambda x: str(x).split(" ")[-1] if not pd.isnull(x) else ""
    )
    return df


@register("contract_line_first_account")
def contract_line_first_account(
    df: pd.DataFrame,
    loader_config: Any,
    dataframes: dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    df["FirstAccount"] = df["DerivedFirstAccount"].apply(lambda x: _split_pipe(x, 3))
    return df


@register("vendor_item")
def vendor_item(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    # The UOM factors and the stamps are typed in the mapping, so
    # apply_type_changes already converted them (thousands separators stripped,
    # numeric blanks kept as NaN -> SQL NULL; the old notebook behavior of
    # forcing 0.0 is retired). String blanks are filled by normalize_for_db.
    df["LastUpdate"] = datetime.today().strftime("%Y-%m-%d")
    return df


@register("manufacturer")
def manufacturer(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    # Column selection, source->destination renaming, and blank-filling are all
    # handled by the mapping pipeline (see manufacturer.yaml); the transform only
    # stamps the load-date marker (mapped to the LastUpdated date column).
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    return df


@register("supplier")
def supplier(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    # Column selection, source->destination renaming, and blank-filling are
    # handled by the mapping pipeline (see supplier.yaml); the transform derives
    # the Supplier code from the representative string (e.g. '1 - PREMIER, INC'
    # -> '1') and stamps the load-date marker.
    df["Supplier"] = df["RepresentativeText"].apply(lambda x: str(x).split(" - ")[0].strip())
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    return df


@register("edi_sub")
def edi_sub(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    df.fillna("NA", inplace=True)
    for col in df.columns:
        df[col] = df[col].astype(str)
    return df


@register("item")
def item(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    # Column selection, int typing of the two multipliers, and blank-filling are
    # all handled by the mapping pipeline (see item.yaml); the transform only
    # stamps the load-date marker.
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    return df


# The CCX export writes ERP vendor # as a 7-digit id, on its own ("1000703") or
# with a location suffix written either "1000703 B001" or "1000703-B001" -- all
# three are valid downstream; today's suffixes run B001-B009. Anything else is a
# data-entry slip in GHX: on 2026-09-10 two rows arrived as
# "1025146 -MERZ PHARMACEUTICAL LLC" -- 32 chars bound against a varchar(20)
# column -- and the driver rejected them at bind time, which aborted the insert
# after the truncate and left both CCXSyncContract tables empty for the day.
_ERP_VENDOR_ALLOWED_FORM = r"^\d{7}([ -][A-Za-z]\d{3})?$"
_ERP_VENDOR_CANONICAL = re.compile(r"^(\d{7})(?:\s*([ -])\s*([A-Za-z]\d{3}))?$")
_ERP_VENDOR_LEADING_ID = re.compile(r"^(\d{7})\b")


def _normalize_erp_vendor_id(value: Any) -> str:
    """Reduce an ``ERP vendor #`` cell to ``0000000``, ``0000000 B000`` or
    ``0000000-B000`` -- the only shapes the column is allowed to hold.

    The separator the source used is preserved; only padding, casing and a
    non-conforming tail are dropped. A value with no leading 7-digit id at all is
    left as-is rather than blanked, so it is not silently discarded: the width
    guard in ``file_loader`` clamps it to fit and names the column on the health
    report, which is the signal to go look at the source export.
    """
    # A blank cell arrives as NA, not '': the reader hands this column back as
    # pandas' str dtype, which _fill_string_blanks (object-dtype only) skips.
    # normalize_for_db lands those on '' anyway, so match it here rather than
    # stringifying NA into a literal "nan".
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    match = _ERP_VENDOR_CANONICAL.match(text)
    if match:
        vendor_id, separator, suffix = match.groups()
        return vendor_id if suffix is None else f"{vendor_id}{separator}{suffix.upper()}"
    leading = _ERP_VENDOR_LEADING_ID.match(text)
    return leading.group(1) if leading else text


@register("ccx_sync_contract")
def ccx_sync_contract(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df = _fill_string_blanks(df)
    df["report stamp"] = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    df["Synced"] = df["Synced"].apply(lambda x: 1 if x == "True" else 0)
    df["Organization"] = df["Organization"].apply(lambda x: str(x).strip())

    vendor = df["ERP vendor #"]
    normalized = vendor.map(_normalize_erp_vendor_id)
    # Compare against the blank-filled original so the NA cells this column is full
    # of (see _normalize_erp_vendor_id) are not counted as repairs.
    original = vendor.fillna("").astype(str)
    repaired = original[normalized != original]
    if not repaired.empty:
        # The health row states the rule, not the data; the run log keeps the
        # offending values so the source export can be traced.
        examples = ", ".join(repr(value) for value in sorted(set(repaired))[:5])
        logger.warning(
            "ERP vendor # held %s value(s) outside %s; rewritten to fit. e.g. %s",
            len(repaired),
            _ERP_VENDOR_ALLOWED_FORM,
            examples,
        )
        record_transform_warning(
            f"ERP VENDOR NORMALIZED (warning), allow {_ERP_VENDOR_ALLOWED_FORM}, "
            f"rewritten to fit, ETL continued"
        )
    df["ERP vendor #"] = normalized
    return df


@register("contract_line_error")
def contract_line_error(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df["ErrorMessageNumber"] = df["ErrorMessageNumber"].astype(int)
    df["ContractLineError"] = df["ContractLineError"].astype(int)
    df["ContractLine.UOM"] = df["ContractLine.UOM"].apply(lambda x: str(x).strip())
    df["ContractLine.UOMConversion"] = df["ContractLine.UOMConversion"].apply(lambda x: int(float(str(x).replace(",", ""))))
    df["create stamp"] = pd.to_datetime(df["create stamp"])
    df["update stamp"] = pd.to_datetime(df["update stamp"])
    return _fill_string_blanks(df)


@register("vendor_location")
def vendor_location(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    # Column selection, datetime typing, and blank-filling are handled by the
    # mapping pipeline (see vendor_location.yaml); the transform only
    # derives the location text from the representative string, e.g.
    # 'B001 - THE NEW ENGLAND JOURNAL OF MEDICINE' -> the part after ' - '.
    df["VendorLocationText"] = df["RepresentativeText"].apply(lambda x: str(x).split(" - ")[-1])
    return df


@register("vendor")
def vendor(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    # Column selection, source->destination renaming, datetime typing of the two
    # stamps, and blank-filling are all handled by the mapping pipeline (see
    # vendor.yaml). This transform does two things the mapping cannot.
    #
    # 1. Drop the export's trailer rows. Unlike every other Infor export in this
    #    repo, the vendor saved search ships with subtotals ON, so the file ends
    #    with two summary lines that are NOT vendors:
    #        'Total for: Vendor Group: 1 (VENDOR GROUP)  1'
    #        'Total'
    #    in the Vendor column, every other field blank. Real vendor numbers are
    #    all digits (5-7 of them in the 2026-09-01 sample), so keeping only
    #    all-digit keys drops the trailers and any future subtotal variant --
    #    a prefix match on 'Total' would not. astype(str) first so a missing key
    #    reads as 'nan' and is dropped rather than raising.
    #
    #    This runs BEFORE pk_check and before the truncate+insert, so a trailer
    #    row can never reach the table or trip the NOT NULL on ReportDate.
    keep = df["Vendor"].astype(str).str.strip().str.fullmatch(r"\d+")
    if not keep.all():
        dropped = df.loc[~keep, "Vendor"].astype(str).tolist()
        logger.info(
            "Dropping %d non-vendor row(s) (export subtotal trailers): %s",
            len(dropped),
            dropped[:10],
        )
        df = df[keep].copy()

    # 2. Stamp the load-date marker (mapped to the ReportDate date column); tells
    #    consumers when this snapshot was last reloaded.
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    return df


@register("requesting_location")
def requesting_location(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    # Column selection, source->destination renaming, datetime typing of the two
    # stamps, and blank-filling are all handled by the mapping pipeline (see
    # requesting_location.yaml); the transform only stamps the load-date marker.
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    return df


@register("commodity_gl")
def commodity_gl(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df.drop(columns=["ItemGroup"], inplace=True)
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    return _fill_string_blanks(df)


@register("requester")
def requester(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df.drop(columns=["Reference1", "Reference2"], inplace=True)
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    return _fill_string_blanks(df)


@register("completed_invoice")
def completed_invoice(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    if "PO Number" in df.columns:
        df["PO Number"] = df["PO Number"].fillna("")
        long_po_ind = df[df["PO Number"].apply(lambda x: len(str(x)) > 40)].index
        df.loc[long_po_ind, "PO Number"] = ""
    return df


def _to_float(value: Any) -> float:
    if pd.isnull(value):
        return np.nan
    return float(str(value).strip().replace(",", ""))


def _to_int(value: Any) -> int | None:
    if pd.isnull(value):
        return None
    return int(float(str(value).strip().replace(",", "")))


def _split_pipe(value: Any, index: int) -> str:
    if pd.isnull(value):
        return ""
    parts = str(value).split("|")
    return parts[index] if len(parts) > index else ""


def _remove_leading_zeros(value: str) -> str:
    if pd.isnull(value) or value == "nan":
        return ""
    index = 0
    while index < len(value) and value[index] == "0":
        index += 1
    return value[index:]
