from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd


Transform = Callable[[pd.DataFrame, Any, dict[str, pd.DataFrame], logging.Logger], pd.DataFrame]
TRANSFORMS: dict[str, Transform] = {}


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


def normalize_for_db(df: pd.DataFrame, type_changes: dict[str, list[str]]) -> pd.DataFrame:
    date_cols = set(type_changes.get("date", []))
    int_cols = set(type_changes.get("int", []))
    numeric_or_datetime = set(type_changes.get("float", [])) | set(type_changes.get("datetime", []))
    numeric_or_datetime.add("report stamp")

    for col in df.columns:
        if col in date_cols:
            df[col] = df[col].fillna("0001-01-01")
        elif col in int_cols:
            df[col] = df[col].astype(object).where(df[col].notna(), None)
        elif col in numeric_or_datetime:
            continue
        else:
            df[col] = df[col].fillna("")
            df[col] = df[col].apply(lambda x: str(x).strip())
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
    options = loader_config.transform_options
    map_dir = Path(options["map_dir"])

    company_map = pd.read_csv(map_dir / "company_map.csv", dtype=str)
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

    fd3 = pd.read_csv(map_dir / "FD3.csv", dtype=str)
    fd5 = pd.read_csv(map_dir / "FD5.csv", dtype=str)
    fd3_m = fd3[["FinanceDimension3", "Description"]].copy()
    fd3_m.columns = ["FD3", "FD3Text"]
    fd5_m = fd5[["FinanceDimension5", "Description"]].copy()
    fd5_m.columns = ["FD5", "FD5Text"]
    df = df.merge(fd3_m, on=["FD3"], how="left").merge(fd5_m, on=["FD5"], how="left")

    inventory_index = df[~df["InventoryFD1"].isnull()].index
    df.loc[inventory_index, "FD5"] = "INVENTORY - " + df.loc[inventory_index, "TransientInventoryLocation"].fillna("")
    df.loc[inventory_index, "FD5Text"] = df.loc[inventory_index, "FD5"]
    df["FD5Text"] = df["FD5Text"].fillna(df["FD1Text"])

    df["PurchaseOrder.Reference1"] = df["PurchaseOrder.Reference1"].apply(lambda x: _remove_leading_zeros(str(x).strip()))
    df["Helper"] = df["PurchaseOrder.Reference1"].apply(_reference_checker)

    still_need_list = set(options.get("still_need_list", []))
    df.loc[df["PurchaseOrder"].isin(still_need_list), "Helper"] = False

    legacy_ind = df[df["Helper"] == False].index
    unreleased_ind = df[
        (df["PurchaseOrder.IsReleased"] == "No")
        & (df["PurchaseOrder.DerivedPurchaseOrderStatus"] != "Closed")
    ].index
    canceled_after_ind = df[
        (df["LineReleased"] == "Yes")
        & (df["PurchaseOrder.DerivedPurchaseOrderStatus"] == "Canceled")
    ].index

    df["EXC_FLAG"] = "Default"
    df.loc[legacy_ind, "EXC_FLAG"] = "Remove - legacy PO transferred"
    df.loc[unreleased_ind, "EXC_FLAG"] = "Remove - unreleased"
    df.loc[canceled_after_ind, "EXC_FLAG"] = "Remove - released but canceled"

    df["System"] = "INFOR"
    df["Year"] = df["PurchaseOrder.MMAHSPOReleaseDate"].apply(lambda x: str(x)[:4] if not pd.isnull(x) else np.nan)
    df["YearMonth"] = df["PurchaseOrder.MMAHSPOReleaseDate"].apply(lambda x: str(x)[:7] if not pd.isnull(x) else np.nan)
    df["Quarter"] = df["YearMonth"].apply(add_quarter)

    year_month_approx_ind = df[(df["YearMonth"].isnull()) & (df["EXC_FLAG"] == "Default")].index
    df.loc[year_month_approx_ind, "Year"] = df.loc[year_month_approx_ind, "MMAHSPurchaseOrderDate"].apply(lambda x: str(x)[:4])
    df.loc[year_month_approx_ind, "YearMonth"] = df.loc[year_month_approx_ind, "MMAHSPurchaseOrderDate"].apply(lambda x: str(x)[:7])
    df.loc[year_month_approx_ind, "Quarter"] = df.loc[year_month_approx_ind, "YearMonth"].apply(add_quarter)
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


@register("par_item")
def par_item(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    for col in ["create stamp", "update stamp"]:
        df[col] = pd.to_datetime(df[col])
    df["BinSequence"] = df["BinSequence"].astype(int)
    df["DefaultTransactionUOM.UOMConversion"] = df["DefaultTransactionUOM.UOMConversion"].apply(lambda x: float(str(x).replace(",", "")))
    return df


@register("vendor_item")
def vendor_item(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df["LastUpdate"] = datetime.today().strftime("%Y-%m-%d")
    for col in ["Item.DefaultBuyUOMMultiplier", "VendorBuyUOM.UOMConversion"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: float(str(x).replace(",", "")) if not pd.isnull(x) else 0.0)
    df = df.fillna("")
    for col in ["create stamp", "update stamp"]:
        df[col] = pd.to_datetime(df[col])
    return df


@register("item_uom")
def item_uom(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    for col in ["UOMConversion", "PackingWeight", "PackingVolume"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: float(str(x).replace(",", "")))
    for col in ["create stamp", "update stamp"]:
        df[col] = pd.to_datetime(df[col])
    return df.fillna("")


@register("manufacturer")
def manufacturer(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df = df[["Manufacturer", "MMAHSManufacturerEID", "Description", "Active"]].copy()
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    return df


@register("supplier")
def supplier(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df["Supplier"] = df["RepresentativeText"].apply(lambda x: str(x).split(" - ")[0].strip())
    df = df[
        [
            "Supplier",
            "RepresentativeText",
            "SupplierName",
            "Vendor",
            "Vendor.VendorName",
            "Vendor.VendorClass",
            "Active",
            "HasBeenValidated",
        ]
    ].copy()
    df = df.fillna("")
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
    cols_to_take = [
        "Item",
        "Active",
        "ConsignCode",
        "Consignment",
        "CriticalItem",
        "DefaultBuyUOM",
        "DefaultBuyUOMMultiplier",
        "DefaultInventoryTransactionUOM",
        "DefaultInventoryTransactionUOMMultiplier",
        "StockUOM",
        "Description",
        "Description3",
        "Discontinued",
        "GenericName",
        "Implantable",
        "Reusable",
        "Sterile",
        "GTINForStockUOM",
        "ItemGTINsRel.Active",
        "HCPCSCode",
        "ItemDescriptionAbbreviation",
        "CommodityCode",
        "CommodityCode.CcDescription",
        "MMAHSPrimaryDI",
        "MajorInventoryClass",
        "MajorPPEClass",
        "MajorPurchasingClass",
        "MajorPurchasingClass.Description",
        "MinorInventoryClass",
        "MinorPPEClass",
        "MinorPurchasingClass",
        "Manufacturer",
        "ManufacturerDescription",
        "ManufacturerNumber",
    ]
    df = df[cols_to_take].copy()
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    for col in ["DefaultBuyUOMMultiplier", "DefaultInventoryTransactionUOMMultiplier"]:
        df[col] = df[col].apply(lambda x: int(float(str(x).replace(",", ""))))
    return df.fillna("")


@register("ccx_sync_contract")
def ccx_sync_contract(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df = df.fillna("")
    df["report stamp"] = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    df["Synced"] = df["Synced"].apply(lambda x: 1 if x == "True" else 0)
    df["Organization"] = df["Organization"].apply(lambda x: str(x).strip())
    return df


@register("contract_line_error")
def contract_line_error(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df["ErrorMessageNumber"] = df["ErrorMessageNumber"].astype(int)
    df["ContractLineError"] = df["ContractLineError"].astype(int)
    df["ContractLine.UOM"] = df["ContractLine.UOM"].apply(lambda x: str(x).strip())
    df["ContractLine.UOMConversion"] = df["ContractLine.UOMConversion"].apply(lambda x: int(float(str(x).replace(",", ""))))
    df["create stamp"] = pd.to_datetime(df["create stamp"])
    df["update stamp"] = pd.to_datetime(df["update stamp"])
    return df.fillna("")


@register("vendor_location")
def vendor_location(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df["VendorLocationText"] = df["RepresentativeText"].apply(lambda x: str(x).split(" - ")[-1])
    cols = ["Vendor", "VendorName", "VendorLocation", "VendorLocationText", "Status", "LocationType", "create stamp", "update stamp"]
    df = df[cols].copy()
    for col in ["create stamp", "update stamp"]:
        df[col] = pd.to_datetime(df[col])
    return df.fillna("")


@register("item_replenish_from")
def item_replenish_from(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df["ItemReplenishmentSource.ReplenishmentPriority"] = df["ItemReplenishmentSource.ReplenishmentPriority"].astype(int)
    for col in ["create stamp", "update stamp"]:
        df[col] = pd.to_datetime(df[col])
    return df.fillna("")


@register("item_gtin")
def item_gtin(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df["UnitOfMeasure.UOMConversion"] = df["UnitOfMeasure.UOMConversion"].apply(lambda x: float(str(x).replace(",", "")))
    for col in ["create stamp", "update stamp"]:
        df[col] = pd.to_datetime(df[col])
    return df.fillna("")


@register("requesting_location")
def requesting_location(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    cols_to_take = [
        "Company",
        "RequestingLocation",
        "Active",
        "DerivedFinanceDimension1",
        "DerivedFinanceDimension3",
        "DerivedFinanceDimension5",
        "DerivedProject",
        "FillOrKill",
        "LocationRule",
        "DerivedFromLocation",
        "FromCompanyLocation",
        "PostalAddress",
        "PostalAddress.DisplayAddressLine1",
        "PostalAddress.DisplayAddressLine2",
        "Name",
        "RequisitionApprovalType",
        "create stamp",
        "update stamp",
    ]
    df = df[cols_to_take].copy()
    for col in ["create stamp", "update stamp"]:
        df[col] = pd.to_datetime(df[col])
    df = df.fillna("")
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    return df


@register("commodity_gl")
def commodity_gl(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df.drop(columns=["ItemGroup"], inplace=True)
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    return df.fillna("")


@register("requester")
def requester(df: pd.DataFrame, loader_config: Any, dataframes: dict[str, pd.DataFrame], logger: logging.Logger) -> pd.DataFrame:
    df.drop(columns=["Reference1", "Reference2"], inplace=True)
    df["ReportDate"] = datetime.now().strftime("%Y-%m-%d")
    return df.fillna("")


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


def _reference_checker(value: str) -> bool:
    if value in ["4502656845 & 4502389074", "4502702803 - 4502536259", "4502689964 & 4502179112"]:
        return False
    if value == "":
        return True

    non_digit_count = sum(1 for char in value if not char.isdigit())
    allowed_prefixes = ("SV", "SA", "RJ", "KH", "DSA", "EB", "ST", "OK", "LS", "PO")
    if non_digit_count >= 2 and not value.startswith(allowed_prefixes):
        return True
    return False
