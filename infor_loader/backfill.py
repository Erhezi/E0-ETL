"""Backfill columns that were added to an already-populated production table.

When a destination table grows columns (an ALTER TABLE ... ADD), the rows already
in it keep NULL there -- the daily loader only refills a row when its key comes
back around in the rolling export window. Waiting that out can take years, so the
history is filled from a one-off export that carries the table's key plus the new
columns, in whatever chunks the reporting tool can hand out.

Two phases, one per run, both driven by a loader-shaped YAML in
``configs/loaders/backfill/``:

  1. STAGE  -- the export chunk is truncate/inserted into a per-destination
     ``*_backfill`` table through the ordinary loader pipeline (rename, type
     conversion, source->destination mapping, PK duplicate check, varchar-width
     clamp, batched fast_executemany insert). The table is created on first use
     from the TARGET table's own column definitions, so the staged types can never
     drift from the columns they will be written into.
  2. MERGE  -- a generated ``MERGE ... WHEN MATCHED THEN UPDATE`` copies the
     configured columns from the backfill table into the target table, joined on
     the target's key. Update-only: a key the target does not have is simply not
     matched and nothing is inserted.

Which columns move, what gates a row and how the values are set all come from the
YAML's ``backfill:`` block, so the same script backfills the next table whose
columns are expanded -- point a new config at a different target and column list.

The merge is run in ``batch_rows``-sized slices of the backfill table's identity
column so a chunk of half a million rows is not one lock-holding, log-growing
transaction against live prod. Each slice commits on its own, so an interrupted
run leaves the earlier slices applied; re-running from the start is safe as long
as the configured gate is idempotent (the recommended "source is not older than
the row" stamp guard is -- it re-applies the same values).

NOTE: unlike the daily loaders this writes NO ETLHealth row. A one-time backfill
is not a scheduled process and would show up on the daily health report as an
unexplained extra job.

>>> This writes to LIVE PROD. Every run prints what it is about to do and asks
>>> for an interactive ``yes`` first; ``--dry-run`` reads and reports only.
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .cli import get_loader, load_loader_configs
from .config import LoadDestination, LoaderConfig, TableRef, bracket_identifier
from .db import connect_sql_server, count_rows, get_table_columns
from .file_loader import FileLoader, build_run_logger, close_logger

#: Identity column the helper adds to every backfill table. Not part of the data:
#: it exists only to slice the merge into batches, and is excluded from the staging
#: insert by the config's ``skip_identity_columns: true``.
ROW_ID_COLUMN = "BackfillRowId"

#: Rows merged per transaction by default. 0 (or ``--batch-rows 0``) merges the
#: whole backfill table in one statement.
DEFAULT_BATCH_ROWS = 50_000

#: How the matched target columns are assigned from the backfill row.
#:   overwrite - tgt.c = src.c: the backfill snapshot wins for every gated row.
#:               Use with an ``update_when`` gate that keeps fresher rows out.
#:   fill_null - tgt.c = COALESCE(tgt.c, src.c): only the holes are filled and a
#:               value already in the target is never touched. For targets with no
#:               version stamp to gate on.
SET_MODES = frozenset({"overwrite", "fill_null"})


@dataclass(frozen=True)
class BackfillSpec:
    """The ``backfill:`` block of a backfill YAML: what the MERGE does.

    ``key`` and ``columns`` are TARGET column names; both must also be staged
    (i.e. appear as a destination in the config's ``field_config.mapping``), which
    is checked before anything is written.
    """

    #: Join columns -- the target's key (e.g. Company + PayablesInvoice).
    key: list[str]
    #: Target columns this backfill writes. The other staged columns (stamps,
    #: anything ``update_when`` reads) ride along in the backfill table but are
    #: never written to the target.
    columns: list[str]
    #: Extra SQL predicate deciding WHICH matched rows are updated, written against
    #: the ``tgt`` (target) and ``src`` (backfill) aliases. None updates every
    #: matched row. See the payablesinvoice_header config for the stamp guard that
    #: keeps a stale snapshot from overwriting a row the daily loader refreshed.
    update_when: str | None = None
    set_mode: str = "overwrite"
    batch_rows: int = DEFAULT_BATCH_ROWS
    #: Per-destination backfill table name, keyed by destination name. Normally
    #: empty: the table is the destination's `staging` block, and this only exists
    #: for a config that wants to override one side's name.
    table_overrides: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BackfillSpec":
        key = [str(column) for column in (data.get("key") or [])]
        columns = [str(column) for column in (data.get("columns") or [])]
        if not key:
            raise ValueError("backfill.key must list the target's join column(s).")
        if not columns:
            raise ValueError("backfill.columns must list the target columns to fill.")
        overlap = sorted(set(key) & set(columns))
        if overlap:
            raise ValueError(
                f"backfill.columns may not include the join key {overlap}; the key "
                f"identifies the row, it is never written."
            )
        set_mode = str(data.get("set_mode", "overwrite")).strip().lower()
        if set_mode not in SET_MODES:
            allowed = ", ".join(sorted(SET_MODES))
            raise ValueError(f"backfill.set_mode must be one of: {allowed}; got {set_mode!r}.")
        batch_rows = int(data.get("batch_rows", DEFAULT_BATCH_ROWS))
        if batch_rows < 0:
            raise ValueError(f"backfill.batch_rows must be >= 0 (0 = one statement); got {batch_rows}.")
        update_when = data.get("update_when")
        return cls(
            key=key,
            columns=columns,
            update_when=str(update_when).strip() if update_when else None,
            set_mode=set_mode,
            batch_rows=batch_rows,
            table_overrides={
                str(name): str(table) for name, table in (data.get("tables") or {}).items()
            },
        )


@dataclass
class BackfillResult:
    """What one destination's backfill run did."""

    loader_name: str
    destination: str
    backfill_table: TableRef
    target_table: TableRef
    status: str  # STAGED | MERGED | DRY_RUN | ABORTED | SKIPPED
    rows_staged: int | None = None
    rows_matched: int | None = None
    rows_eligible: int | None = None
    rows_updated: int | None = None
    warning: str | None = None
    message: str | None = None


def load_backfill_config(config_ref: str, loader_name: str) -> tuple[LoaderConfig, BackfillSpec]:
    """Load a backfill YAML and its ``backfill:`` block.

    The YAML is an ordinary :class:`LoaderConfig` -- ``staging`` is the backfill
    table and ``prod`` is the table being filled -- so it reuses the loader
    mapping/type machinery verbatim and is discovered by the same config loader.
    It carries ``enabled: false`` so the loader CLI never runs it as a loader; this
    helper reads it regardless.
    """
    configs = load_loader_configs(config_ref)
    config = get_loader(configs, loader_name)
    raw = _raw_backfill_block(config_ref, loader_name)
    if raw is None:
        raise ValueError(
            f"Loader {loader_name!r} has no `backfill:` block; it is not a backfill config. "
            f"Backfill configs live in configs/loaders/backfill/."
        )
    return config, BackfillSpec.from_dict(raw)


def _raw_backfill_block(config_ref: str, loader_name: str) -> dict[str, Any] | None:
    """Re-read the YAML for the ``backfill:`` block.

    :class:`LoaderConfig` keeps only the keys the loaders use and drops the rest,
    so the block is read from the file itself. Searched the same way
    ``load_loader_configs`` discovers configs: a directory recurses into
    ``loaders/``, a path to a single YAML is read directly.
    """
    import yaml

    config_path = Path(config_ref)
    if config_path.is_file():
        candidates = [config_path]
    else:
        loader_dir = config_path / "loaders" if (config_path / "loaders").is_dir() else config_path
        candidates = sorted([*loader_dir.rglob("*.yaml"), *loader_dir.rglob("*.yml")])
    for path in candidates:
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001 - a sibling config that will not parse is not ours.
            continue
        if isinstance(data, dict) and data.get("name") == loader_name:
            block = data.get("backfill")
            return dict(block) if isinstance(block, dict) else None
    raise ValueError(f"Could not find the YAML for loader {loader_name!r} under {config_ref}.")


def staged_columns(config: LoaderConfig) -> list[str]:
    """Destination column names the config's mapping stages into the backfill table,
    in mapping order."""
    columns = [
        str(item.get("destination") or item.get("dest"))
        for item in (config.column_mapping or [])
        if (item.get("destination") or item.get("dest")) and item.get("type") != "ignore"
    ]
    if not columns:
        raise ValueError(
            f"{config.name}: field_config.mapping is empty; the backfill table has no "
            f"columns to stage."
        )
    return columns


def backfill_table_for(destination: LoadDestination, spec: BackfillSpec) -> TableRef:
    """This destination's backfill table: its ``staging`` block, or the name
    ``backfill.tables`` overrides it with."""
    override = spec.table_overrides.get(destination.name)
    return replace(destination.staging, table=override) if override else destination.staging


def target_table_for(config: LoaderConfig, destination: LoadDestination) -> TableRef:
    """This destination's table being backfilled: its ``prod`` block."""
    if destination.prod is None:
        raise ValueError(
            f"{config.name}: destination {destination.name!r} declares no `prod` table, so there "
            f"is nothing to backfill into. A backfill config's `prod` block names the table "
            f"being filled and `staging` names the backfill table."
        )
    return destination.prod


# --------------------------------------------------------------------------- #
# Backfill table DDL, derived from the target table                            #
# --------------------------------------------------------------------------- #


def _render_column_type(column: dict[str, Any]) -> str:
    """SQL type for a backfill column, copied from the target table's metadata so
    the staged value is never widened, narrowed or rounded on its way across."""
    data_type = str(column.get("data_type") or "").lower()
    if not data_type:
        raise ValueError(
            f"Target column {column.get('name')!r} reports no data type; the backfill "
            f"table cannot be created from it. Create it by hand and re-run with --no-create."
        )
    if data_type in {"varchar", "char", "nvarchar", "nchar", "binary", "varbinary"}:
        length = column.get("max_length")
        # INFORMATION_SCHEMA reports -1 for the (max) types.
        size = "max" if length in (None, -1) else str(int(length))
        return f"{data_type}({size})"
    if data_type in {"decimal", "numeric"}:
        precision = int(column.get("numeric_precision") or 18)
        scale = int(column.get("numeric_scale") or 0)
        return f"{data_type}({precision},{scale})"
    # datetime2/time/datetimeoffset are emitted without an explicit precision, which
    # defaults to the widest (7) -- a superset of whatever the target uses, so no
    # staged value loses precision on the way in.
    return data_type


def build_create_table_sql(
    backfill_table: TableRef,
    target_columns: list[dict[str, Any]],
    columns: list[str],
    key: list[str],
) -> str:
    """CREATE TABLE for the backfill table: the staged columns typed exactly as the
    target types them, every one NULL-able (a blank in the export is a blank), plus
    the :data:`ROW_ID_COLUMN` identity that slices the merge into batches.

    Clustered on the row id so a batch is a contiguous range scan, with a
    non-clustered index on the join key for the merge's join to the target.
    """
    by_name = {column["name"]: column for column in target_columns}
    missing = [name for name in columns if name not in by_name]
    if missing:
        raise ValueError(
            f"Target {backfill_table.display_name()} has no column(s) {missing}; the backfill "
            f"table is built from the target's own definitions, so every staged column must "
            f"exist there. Add the columns to the target first."
        )
    definitions = [f"    {bracket_identifier(ROW_ID_COLUMN)} int IDENTITY(1,1) NOT NULL"]
    definitions += [
        f"    {bracket_identifier(name)} {_render_column_type(by_name[name])} NULL"
        for name in columns
    ]
    definitions.append(
        f"    CONSTRAINT {bracket_identifier('PK_' + backfill_table.table)} "
        f"PRIMARY KEY CLUSTERED ({bracket_identifier(ROW_ID_COLUMN)})"
    )
    key_columns = ", ".join(bracket_identifier(name) for name in key)
    index_name = bracket_identifier(f"IX_{backfill_table.table}_key")
    qualified = backfill_table.qualified_name()
    return (
        f"IF OBJECT_ID('{qualified}', 'U') IS NULL\n"
        f"BEGIN\n"
        f"CREATE TABLE {qualified} (\n" + ",\n".join(definitions) + "\n);\n"
        f"CREATE NONCLUSTERED INDEX {index_name} ON {qualified} ({key_columns});\n"
        f"END;"
    )


def ensure_backfill_table(
    cnxn: Any,
    backfill_table: TableRef,
    target_table: TableRef,
    columns: list[str],
    key: list[str],
    *,
    out: Callable[[str], None] = print,
) -> str:
    """Create the backfill table from the target's column definitions if it is not
    there yet. Returns the DDL that was (or would be) run.

    Both tables are read on ``cnxn``: a backfill config's staging and prod blocks
    are the same server and database, which is what lets one MERGE see both.
    """
    target_columns = get_table_columns(cnxn, target_table)
    ddl = build_create_table_sql(backfill_table, target_columns, columns, key)
    cursor = cnxn.cursor()
    exists = cursor.execute(
        "SELECT OBJECT_ID(?, 'U')", backfill_table.qualified_name()
    ).fetchone()[0]
    if exists is not None:
        out(f"  Backfill table {backfill_table.display_name()} already exists; reusing it.")
        cursor.close()
        return ddl
    out(f"  Creating backfill table {backfill_table.display_name(include_server=True)}...")
    cursor.execute(ddl)
    cnxn.commit()
    cursor.close()
    return ddl


def verify_backfill_table(cnxn: Any, backfill_table: TableRef, columns: list[str]) -> None:
    """Fail early if an existing backfill table is missing a staged column -- e.g.
    it was left over from an earlier config with a shorter column list."""
    present = {column["name"] for column in get_table_columns(cnxn, backfill_table)}
    missing = [name for name in columns if name not in present]
    if missing:
        raise ValueError(
            f"Backfill table {backfill_table.display_name()} is missing column(s) {missing}. "
            f"It predates this config; DROP it and re-run so it is rebuilt from the target."
        )


# --------------------------------------------------------------------------- #
# MERGE generation                                                             #
# --------------------------------------------------------------------------- #


def _join_predicate(key: list[str]) -> str:
    return "\n    AND ".join(
        f"tgt.{bracket_identifier(column)} = src.{bracket_identifier(column)}" for column in key
    )


def _source_select(backfill_table: TableRef, columns: list[str], *, batched: bool) -> str:
    column_sql = ", ".join(bracket_identifier(column) for column in columns)
    where = (
        f"\n        WHERE {bracket_identifier(ROW_ID_COLUMN)} >= ? "
        f"AND {bracket_identifier(ROW_ID_COLUMN)} < ?"
        if batched
        else ""
    )
    return f"        SELECT {column_sql}\n        FROM {backfill_table.qualified_name()}{where}"


def build_merge_sql(
    target_table: TableRef,
    backfill_table: TableRef,
    spec: BackfillSpec,
    source_columns: list[str],
    *,
    batched: bool = True,
) -> str:
    """The update-only MERGE that fills ``spec.columns`` in the target.

    ``WHEN NOT MATCHED`` is deliberately absent: a backfill fills columns on rows
    the target already has. A key in the export that the target does not carry (an
    invoice outside its retained window) is left alone rather than inserted as a
    row with almost every column NULL.

    ``NOCOUNT`` is on so the trailing ``SELECT @@ROWCOUNT`` is the batch's only
    result set, and reports exactly the rows the MERGE updated.
    """
    if spec.set_mode == "fill_null":
        assignments = [
            f"    tgt.{bracket_identifier(column)} = "
            f"COALESCE(tgt.{bracket_identifier(column)}, src.{bracket_identifier(column)})"
            for column in spec.columns
        ]
    else:
        assignments = [
            f"    tgt.{bracket_identifier(column)} = src.{bracket_identifier(column)}"
            for column in spec.columns
        ]
    matched_when = f" AND ({spec.update_when})" if spec.update_when else ""
    return (
        "SET NOCOUNT ON;\n"
        f"MERGE INTO {target_table.qualified_name()} AS tgt\n"
        "USING (\n"
        f"{_source_select(backfill_table, source_columns, batched=batched)}\n"
        ") AS src\n"
        f"    ON  {_join_predicate(spec.key)}\n"
        f"WHEN MATCHED{matched_when} THEN UPDATE SET\n"
        + ",\n".join(assignments)
        + "\n;\nSELECT @@ROWCOUNT;"
    )


def build_preview_sql(target_table: TableRef, backfill_table: TableRef, spec: BackfillSpec) -> str:
    """Count what the merge would touch, without touching it: how many backfill
    rows find their key in the target (``matched``), and how many of those pass the
    ``update_when`` gate (``eligible``)."""
    gate = spec.update_when or "1 = 1"
    return (
        "SELECT COUNT_BIG(*) AS matched,\n"
        f"       SUM(CASE WHEN {gate} THEN 1 ELSE 0 END) AS eligible\n"
        f"FROM {backfill_table.qualified_name()} AS src\n"
        f"JOIN {target_table.qualified_name()} AS tgt\n"
        f"    ON  {_join_predicate(spec.key)}"
    )


def _row_id_range(cnxn: Any, backfill_table: TableRef) -> tuple[int, int]:
    """(min, max) of the backfill table's row id, or (0, -1) when it is empty."""
    cursor = cnxn.cursor()
    row = cursor.execute(
        f"SELECT MIN({bracket_identifier(ROW_ID_COLUMN)}), MAX({bracket_identifier(ROW_ID_COLUMN)}) "
        f"FROM {backfill_table.qualified_name()}"
    ).fetchone()
    cursor.close()
    if row is None or row[0] is None:
        return 0, -1
    return int(row[0]), int(row[1])


def run_merge(
    cnxn: Any,
    target_table: TableRef,
    backfill_table: TableRef,
    spec: BackfillSpec,
    source_columns: list[str],
    *,
    out: Callable[[str], None] = print,
) -> int:
    """Run the MERGE and return the rows updated.

    Batched in ``spec.batch_rows``-sized slices of the row id, each its own
    transaction, so a half-million-row chunk does not hold locks on a live prod
    table for the length of one statement. An interrupted run leaves the completed
    slices applied -- re-run from the start, which is a no-op for anything already
    filled as long as the configured gate is idempotent.
    """
    batched = spec.batch_rows > 0
    merge_sql = build_merge_sql(
        target_table, backfill_table, spec, source_columns, batched=batched
    )
    cursor = cnxn.cursor()
    try:
        if not batched:
            cursor.execute(merge_sql)
            updated = int(cursor.fetchone()[0] or 0)
            cnxn.commit()
            out(f"  Merged in one statement: {updated:,} row(s) updated.")
            return updated

        low, high = _row_id_range(cnxn, backfill_table)
        if high < low:
            out("  Backfill table is empty; nothing to merge.")
            return 0
        updated = 0
        start = low
        while start <= high:
            stop = start + spec.batch_rows
            cursor.execute(merge_sql, start, stop)
            updated += int(cursor.fetchone()[0] or 0)
            cnxn.commit()
            done = min(stop - low, high - low + 1)
            out(f"  ... {done:,}/{high - low + 1:,} backfill rows merged, {updated:,} updated.")
            start = stop
        return updated
    finally:
        cursor.close()


# --------------------------------------------------------------------------- #
# Orchestration                                                                #
# --------------------------------------------------------------------------- #


def _with_source_file(config: LoaderConfig, file_path: Path) -> LoaderConfig:
    """Point the config's primary source file at ``file_path``.

    Each export chunk is a separate file, so the file is named on the command line
    rather than in the YAML; the config keeps only the folder it is expected in.
    """
    source = config.source_files[0]
    rebound = replace(
        source,
        path=str(file_path.parent),
        name=file_path.name,
        pick_latest=False,
        input_key=None,
    )
    return replace(config, source_files=[rebound, *config.source_files[1:]])


def _preview(cnxn: Any, target_table: TableRef, backfill_table: TableRef, spec: BackfillSpec) -> tuple[int, int]:
    cursor = cnxn.cursor()
    try:
        row = cursor.execute(build_preview_sql(target_table, backfill_table, spec)).fetchone()
    finally:
        cursor.close()
    if row is None or row[0] is None:
        return 0, 0
    return int(row[0]), int(row[1] or 0)


def run_backfill(
    config: LoaderConfig,
    spec: BackfillSpec,
    *,
    file_path: Path | None = None,
    destination_names: list[str] | None = None,
    stage: bool = True,
    merge: bool = True,
    create_table: bool = True,
    dry_run: bool = False,
    assume_yes: bool = False,
    batch_rows: int | None = None,
    out: Callable[[str], None] = print,
) -> list[BackfillResult]:
    """Stage one export chunk and merge it into the target on every selected
    destination, in destination order.

    Both phases run on the same connection per destination: the backfill table is
    the destination's ``staging`` block and the target is its ``prod`` block, which
    share a server and database, so the MERGE sees both without a linked server.
    """
    if batch_rows is not None:
        spec = replace(spec, batch_rows=batch_rows)
    columns = staged_columns(config)
    _validate_spec_against_mapping(config, spec, columns)

    destinations = _select_destinations(config, destination_names)
    if stage and file_path is None:
        raise ValueError("Staging needs a file: pass --file <export chunk>, or --merge-only.")

    logger, log_file_path, _ = build_run_logger(
        name=config.name,
        process_id=uuid.uuid4().hex[:32],
        log_root=Path(config.log_root),
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        to_console=False,
    )
    out(f"Backfill: {config.name}")
    out(f"  Log: {log_file_path}")
    try:
        # Read and prepare the chunk ONCE (the prepared frame is identical for every
        # destination), exactly as the daily loaders prepare their export.
        loader = FileLoader(
            _with_source_file(config, file_path) if stage else config, capture_streams=False
        )
        df: pd.DataFrame | None = None
        if stage:
            out(f"  Source: {file_path}")
            df, _, source_warning = loader.read_and_prepare(logger)
            out(f"  Prepared {len(df):,} row(s), {len(df.columns)} column(s).")
            if source_warning:
                out(f"  WARNING: {source_warning}")

        _print_plan(config, spec, destinations, columns, stage=stage, merge=merge, out=out)
        if dry_run:
            return _dry_run(
                config, spec, destinations, columns, df,
                create_table=create_table, merge=merge, out=out,
            )
        if not assume_yes and not _confirm(config, spec, destinations, merge=merge, out=out):
            return [
                BackfillResult(
                    loader_name=config.name,
                    destination=destination.name,
                    backfill_table=backfill_table_for(destination, spec),
                    target_table=target_table_for(config, destination),
                    status="ABORTED",
                    message="User did not confirm.",
                )
                for destination in destinations
            ]

        results: list[BackfillResult] = []
        for destination in destinations:
            results.append(
                _run_destination(
                    loader, config, spec, destination, columns, df, logger,
                    stage=stage, merge=merge, create_table=create_table, out=out,
                )
            )
        return results
    finally:
        close_logger(logger)


def _run_destination(
    loader: FileLoader,
    config: LoaderConfig,
    spec: BackfillSpec,
    destination: LoadDestination,
    columns: list[str],
    df: pd.DataFrame | None,
    logger: logging.Logger,
    *,
    stage: bool,
    merge: bool,
    create_table: bool,
    out: Callable[[str], None],
) -> BackfillResult:
    backfill_table = backfill_table_for(destination, spec)
    target_table = target_table_for(config, destination)
    result = BackfillResult(
        loader_name=config.name,
        destination=destination.name,
        backfill_table=backfill_table,
        target_table=target_table,
        status="SKIPPED",
    )
    out("")
    out(f"[{destination.name}] {target_table.display_name(include_server=True)}")
    cnxn = connect_sql_server(backfill_table.server, backfill_table.database)
    try:
        if stage:
            if create_table:
                ensure_backfill_table(
                    cnxn, backfill_table, target_table, columns, spec.key, out=out
                )
            verify_backfill_table(cnxn, backfill_table, columns)
            rows, warning = loader.stage_frame(cnxn, backfill_table, df, logger)
            result.rows_staged = rows
            result.warning = warning
            result.status = "STAGED"
            out(f"  Staged {rows:,} row(s) into {backfill_table.display_name()}.")
            if warning:
                out(f"  WARNING: {warning}")
        else:
            verify_backfill_table(cnxn, backfill_table, columns)
            result.rows_staged = count_rows(cnxn, backfill_table)
            out(f"  Using the {result.rows_staged:,} row(s) already in {backfill_table.display_name()}.")

        if merge:
            matched, eligible = _preview(cnxn, target_table, backfill_table, spec)
            result.rows_matched = matched
            result.rows_eligible = eligible
            out(f"  Keys found in the target: {matched:,}; passing the update gate: {eligible:,}.")
            if matched > eligible:
                out(
                    f"  {matched - eligible:,} matched row(s) skipped by the gate "
                    f"(the target holds a version this backfill must not overwrite)."
                )
            result.rows_updated = run_merge(
                cnxn, target_table, backfill_table, spec, columns, out=out
            )
            result.status = "MERGED"
            out(f"  Updated {result.rows_updated:,} row(s) in {target_table.display_name()}.")
    finally:
        cnxn.close()
    return result


def _select_destinations(config: LoaderConfig, names: list[str] | None) -> list[LoadDestination]:
    destinations = [destination for destination in config.destinations if destination.enabled]
    if not names:
        if not destinations:
            raise ValueError(f"{config.name}: every destination is disabled.")
        return destinations
    wanted = set(names)
    available = {destination.name for destination in config.destinations}
    unknown = sorted(wanted - available)
    if unknown:
        raise ValueError(
            f"No destination named {', '.join(unknown)} in {config.name!r}. "
            f"Available: {', '.join(sorted(available))}"
        )
    selected = [destination for destination in destinations if destination.name in wanted]
    if not selected:
        raise ValueError(f"{config.name}: --destination {' '.join(sorted(wanted))} are all disabled.")
    return selected


def _validate_spec_against_mapping(config: LoaderConfig, spec: BackfillSpec, columns: list[str]) -> None:
    """Every key/column the MERGE names must actually be staged by the mapping,
    caught here rather than as a SQL error halfway through a prod write."""
    staged = set(columns)
    missing_key = [column for column in spec.key if column not in staged]
    missing_columns = [column for column in spec.columns if column not in staged]
    if missing_key or missing_columns:
        raise ValueError(
            f"{config.name}: backfill.key {missing_key} / backfill.columns {missing_columns} "
            f"are not produced by field_config.mapping. Staged columns are: {sorted(staged)}."
        )


def _print_plan(
    config: LoaderConfig,
    spec: BackfillSpec,
    destinations: list[LoadDestination],
    columns: list[str],
    *,
    stage: bool,
    merge: bool,
    out: Callable[[str], None],
) -> None:
    phases = " + ".join(phase for phase, on in (("stage", stage), ("merge", merge)) if on)
    out(f"  Phases: {phases}")
    out(f"  Join key: {', '.join(spec.key)}")
    out(f"  Filling ({len(spec.columns)}): {', '.join(spec.columns)}")
    carried = [column for column in columns if column not in {*spec.key, *spec.columns}]
    if carried:
        out(f"  Staged but not written: {', '.join(carried)}")
    out(f"  Set mode: {spec.set_mode}")
    out(f"  Update gate: {spec.update_when or '(none -- every matched row is updated)'}")
    out(f"  Merge batch: {spec.batch_rows or 'one statement'}")
    for destination in destinations:
        out(
            f"  {destination.name}: backfill="
            f"{backfill_table_for(destination, spec).display_name(include_server=True)} "
            f"-> target={target_table_for(config, destination).display_name()}"
        )


def _dry_run(
    config: LoaderConfig,
    spec: BackfillSpec,
    destinations: list[LoadDestination],
    columns: list[str],
    df: pd.DataFrame | None,
    *,
    create_table: bool,
    merge: bool,
    out: Callable[[str], None],
) -> list[BackfillResult]:
    """Report only. Prints the DDL and MERGE each destination would run, and -- when
    the backfill table is already populated -- how many rows the merge would touch."""
    results: list[BackfillResult] = []
    for destination in destinations:
        backfill_table = backfill_table_for(destination, spec)
        target_table = target_table_for(config, destination)
        result = BackfillResult(
            loader_name=config.name,
            destination=destination.name,
            backfill_table=backfill_table,
            target_table=target_table,
            status="DRY_RUN",
            rows_staged=None if df is None else len(df),
        )
        out("")
        out(f"[{destination.name}] dry run -- nothing is written.")
        cnxn = connect_sql_server(backfill_table.server, backfill_table.database)
        try:
            target_columns = get_table_columns(cnxn, target_table)
            if create_table:
                out("  -- backfill table DDL " + "-" * 50)
                out(build_create_table_sql(backfill_table, target_columns, columns, spec.key))
            cursor = cnxn.cursor()
            exists = cursor.execute(
                "SELECT OBJECT_ID(?, 'U')", backfill_table.qualified_name()
            ).fetchone()[0]
            cursor.close()
            if merge:
                out("  -- merge " + "-" * 57)
                out(
                    build_merge_sql(
                        target_table, backfill_table, spec, columns, batched=spec.batch_rows > 0
                    )
                )
                if exists is None:
                    out("  Backfill table does not exist yet; no row counts to preview.")
                else:
                    verify_backfill_table(cnxn, backfill_table, columns)
                    staged_rows = count_rows(cnxn, backfill_table)
                    matched, eligible = _preview(cnxn, target_table, backfill_table, spec)
                    result.rows_matched = matched
                    result.rows_eligible = eligible
                    out(
                        f"  Backfill table holds {staged_rows:,} row(s); {matched:,} match the "
                        f"target, {eligible:,} would be updated."
                    )
        finally:
            cnxn.close()
        results.append(result)
    return results


def _confirm(
    config: LoaderConfig,
    spec: BackfillSpec,
    destinations: list[LoadDestination],
    *,
    merge: bool,
    out: Callable[[str], None],
) -> bool:
    """Ask before writing. A --stage-only run only truncates and reloads its own
    scratch table, so say that instead of raising a prod alarm it does not warrant."""
    out("")
    if merge:
        targets = ", ".join(
            target_table_for(config, destination).display_name(include_server=True)
            for destination in destinations
        )
        out(f"About to write columns {', '.join(spec.columns)} into LIVE PROD table(s): {targets}.")
    else:
        tables = ", ".join(
            backfill_table_for(destination, spec).display_name(include_server=True)
            for destination in destinations
        )
        out(f"About to create/TRUNCATE and reload the backfill table(s): {tables}. Prod is untouched.")
    try:
        answer = input("Type 'yes' to proceed: ")
    except EOFError:
        answer = ""
    if answer.strip().lower() == "yes":
        return True
    out("  Aborted; nothing was written.")
    return False


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #


def _parse_args(argv: list[str]) -> tuple[argparse.Namespace, str]:
    parser = argparse.ArgumentParser(
        prog="backfill_helper",
        description=(
            "Fill columns added to an already-populated prod table from an export chunk: "
            "stage the chunk in a per-destination backfill table, then MERGE the columns "
            "into the target on its key."
        ),
        epilog=(
            "Example: backfill_helper --loader payablesinvoice_header_backfill "
            '--file "...\\invoice_additional_cols_backfill_2026.csv" --dry-run'
        ),
    )
    parser.add_argument("--config", default="configs", help="Config directory or .yaml file (default: configs).")
    parser.add_argument("--loader", default=None, help="Backfill config name, e.g. payablesinvoice_header_backfill.")
    parser.add_argument(
        "--file", default=None,
        help="The export chunk to stage. Each chunk is a separate run: stage it, merge it, "
             "then come back with the next one.",
    )
    parser.add_argument(
        "--destination", action="extend", nargs="+", default=[],
        help="Run only the named destination(s), e.g. des1. Default: every enabled destination.",
    )
    phases = parser.add_mutually_exclusive_group()
    phases.add_argument(
        "--stage-only", action="store_true",
        help="Load the chunk into the backfill table and stop; leave the target untouched.",
    )
    phases.add_argument(
        "--merge-only", action="store_true",
        help="Merge the backfill table already loaded by a previous --stage-only run; read no file.",
    )
    parser.add_argument(
        "--no-create", action="store_true",
        help="Do not create the backfill table when it is missing (fail instead). Use when it "
             "was created by hand.",
    )
    parser.add_argument("--yes", "-y", action="store_true", help="Skip the confirmation prompt.")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Read and report only: print the DDL and MERGE and, if the backfill table is "
             "already loaded, how many rows the merge would update. Never writes.",
    )
    parser.add_argument(
        "--batch-rows", type=int, default=None,
        help=f"Override the config's merge batch size (0 = one statement). Default: the "
             f"config's, else {DEFAULT_BATCH_ROWS}.",
    )

    args, extras = parser.parse_known_args(argv)

    # The config name may be given bare or as --<name>, matching data_fill_helper.
    loader_name = args.loader
    unrecognized: list[str] = []
    for token in extras:
        candidate = token[2:] if token.startswith("--") else token
        if loader_name is None and candidate:
            loader_name = candidate
        else:
            unrecognized.append(token)
    if unrecognized:
        parser.error(f"unrecognized arguments: {' '.join(unrecognized)}")
    if not loader_name:
        parser.error("no backfill config specified; pass one, e.g. --payablesinvoice_header_backfill")
    return args, loader_name


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    args, loader_name = _parse_args(argv)

    try:
        config, spec = load_backfill_config(args.config, loader_name)
        file_path = Path(args.file).expanduser() if args.file else None
        if file_path is not None and not file_path.exists():
            raise FileNotFoundError(str(file_path))
        results = run_backfill(
            config,
            spec,
            file_path=file_path,
            destination_names=args.destination,
            stage=not args.merge_only,
            merge=not args.stage_only,
            create_table=not args.no_create,
            dry_run=args.dry_run,
            assume_yes=args.yes,
            batch_rows=args.batch_rows,
        )
    except Exception as exc:  # noqa: BLE001 - surface a clean message instead of a traceback.
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print()
    for result in results:
        print(
            f"{result.status}\t{result.loader_name}\t{result.destination}\t"
            f"staged={result.rows_staged}\tmatched={result.rows_matched}\t"
            f"eligible={result.rows_eligible}\tupdated={result.rows_updated}"
        )
    return 0 if all(result.status != "ABORTED" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
