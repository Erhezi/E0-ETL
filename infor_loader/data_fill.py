"""Pre-populate one destination's PROD table from another destination's PROD table.

Given a loader config (e.g. ``inventory_location``), this reads rows from the
source destination's prod table and inserts them into the target destination's
prod table. Unlike the file loaders, the input here is an existing prod table,
not a CSV/Excel file -- it is meant for a one-time pre-population.

For a DIRECT-load loader (e.g. the ``ppe_*`` reference tables) a destination has
no separate prod table -- its single configured table IS the production table --
so the effective table is ``prod or staging``, and the copy works the same way.

A loader whose promotion writes SEVERAL prod tables declares the extra ones under
each destination's ``prod.aux`` (name -> table); the same name is the same logical
table on every destination. ``--table <name>`` copies one of them and
``--all-tables`` copies every one in declaration order, primary table first. With
neither, the destination's own prod table (``main``) is copied, as before.

Write mode is *insert only if empty*: the target prod table must have zero rows,
otherwise the copy is skipped without writing. Pass ``--truncate`` to instead
TRUNCATE the target prod table first and then fill it regardless of its current
contents. By default the copy asks for an interactive ``yes`` confirmation
before touching prod; ``--yes`` skips the prompt and ``--dry-run`` reads/reports
only and never writes.
"""

from __future__ import annotations

import argparse
import itertools
import sys
from dataclasses import dataclass
from typing import Callable

from .cli import get_loader, load_loader_configs
from .config import MAIN_TABLE_KEY, LoadDestination, LoaderConfig, TableRef
from .db import (
    connect_sql_server,
    count_rows,
    get_insert_columns,
    insert_dataframe,
    read_table_chunks,
    truncate_table,
)

DEFAULT_CHUNK_SIZE = 500_000


@dataclass
class CopyResult:
    loader_name: str
    # Which of the loader's prod tables this copy was for: MAIN_TABLE_KEY for the
    # destination's own prod table, else the `prod.aux` name.
    table_name: str
    source: TableRef
    dest: TableRef
    copy_columns: list[str]
    source_row_count: int
    dest_row_count_before: int
    rows_copied: int | None
    status: str  # COPIED | DRY_RUN | SKIPPED_NONEMPTY | ABORTED
    message: str | None = None
    rows_truncated: int | None = None


def _resolve_destination(
    config: LoaderConfig,
    name: str | None,
    *,
    role: str,
    default_index: int,
) -> LoadDestination:
    destinations = config.destinations
    if name:
        for destination in destinations:
            if destination.name == name:
                return destination
        available = ", ".join(destination.name for destination in destinations)
        raise ValueError(
            f"No {role} destination named {name!r} in loader {config.name!r}. Available: {available}"
        )
    if len(destinations) <= default_index:
        raise ValueError(
            f"Loader {config.name!r} has no {role} destination at position {default_index + 1}; "
            "pass --from/--to explicitly."
        )
    return destinations[default_index]


def _resolve_prod_table(
    destination: LoadDestination,
    table_name: str,
    *,
    role: str,
    loader: str,
) -> TableRef:
    """One destination's copy of the named production table (see
    :attr:`LoadDestination.prod_tables`)."""
    tables = destination.prod_tables
    if table_name in tables:
        return tables[table_name]
    available = ", ".join(tables)
    raise ValueError(
        f"Loader {loader!r} {role} destination {destination.name!r} has no prod table "
        f"named {table_name!r}. Available: {available}"
    )


def prod_table_names(
    config: LoaderConfig,
    source_name: str | None = None,
    dest_name: str | None = None,
) -> list[str]:
    """Names of the prod tables copyable between this loader's two destinations, in
    declaration order (``main`` first, then each ``prod.aux`` entry). Config already
    requires the aux names to match across enabled destinations, so this is normally
    every name; it is intersected anyway so a disabled/odd destination cannot make
    ``--all-tables`` ask for a table the target does not have."""
    source_dest = _resolve_destination(config, source_name, role="source", default_index=0)
    target_dest = _resolve_destination(config, dest_name, role="target", default_index=1)
    target_names = set(target_dest.prod_tables)
    return [name for name in source_dest.prod_tables if name in target_names]


def copy_prod_table(
    config: LoaderConfig,
    *,
    source_name: str | None = None,
    dest_name: str | None = None,
    table_name: str = MAIN_TABLE_KEY,
    dry_run: bool = False,
    assume_yes: bool = False,
    truncate: bool = False,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    out: Callable[[str], None] = print,
) -> CopyResult:
    """Copy the source destination's prod table into the target destination's prod table.

    ``table_name`` picks WHICH prod table when the loader declares several: the
    destination's own ``prod`` table under :data:`MAIN_TABLE_KEY` (the default), or
    a ``prod.aux`` name, resolved to that destination's copy of it.

    By default inserts only when the target prod table is empty. When ``truncate``
    is ``True``, the target prod table is TRUNCATEd first and then filled
    regardless of its current contents. The source is streamed ``chunk_size``
    rows at a time so the whole table is never held in memory. Returns a
    :class:`CopyResult` describing what happened; never raises for the
    "destination not empty" case.
    """
    source_dest = _resolve_destination(config, source_name, role="source", default_index=0)
    target_dest = _resolve_destination(config, dest_name, role="target", default_index=1)

    if source_dest.name == target_dest.name:
        raise ValueError(f"Source and target are the same destination ({source_dest.name!r}); nothing to copy.")

    # Each destination's production table(s), keyed by name: `main` is the `prod`
    # table -- or, for a DIRECT-load destination (`prod` is None), the single
    # configured table, which IS the final production table (mirrors file_loader's
    # `prod or staging` idiom, so this tool pre-populates direct loaders such as the
    # ppe_* tables too). Any `prod.aux` tables sit alongside under their own names.
    source_table = _resolve_prod_table(source_dest, table_name, role="source", loader=config.name)
    target_table = _resolve_prod_table(target_dest, table_name, role="target", loader=config.name)

    out(f"Loader: {config.name}  (prod table: {table_name})")
    out(f"  FROM {source_dest.name}: {source_table.display_name(include_server=True)}")
    out(f"  TO   {target_dest.name}: {target_table.display_name(include_server=True)}")

    # Resolve which columns to copy: target's insertable (non-identity) columns
    # that also exist in the source table. Read source row count at the same time.
    src_cnxn = connect_sql_server(source_table.server, source_table.database)
    try:
        source_columns = get_insert_columns(src_cnxn, source_table)
        source_row_count = count_rows(src_cnxn, source_table)
    finally:
        src_cnxn.close()

    dst_cnxn = connect_sql_server(target_table.server, target_table.database)
    try:
        dest_insert_columns = get_insert_columns(dst_cnxn, target_table, skip_identity_columns=True)
        dest_row_count = count_rows(dst_cnxn, target_table)
    finally:
        dst_cnxn.close()

    source_set = set(source_columns)
    copy_columns = [column for column in dest_insert_columns if column in source_set]
    missing_in_source = [column for column in dest_insert_columns if column not in source_set]
    if not copy_columns:
        raise ValueError(
            f"No overlapping columns between source {source_table.display_name()} "
            f"and target {target_table.display_name()}."
        )
    if missing_in_source:
        out(f"  WARNING: target columns not found in source (left unset): {missing_in_source}")

    out(f"  Columns to copy ({len(copy_columns)}): {', '.join(copy_columns)}")
    out(f"  Source rows: {source_row_count}")
    out(f"  Target rows (before): {dest_row_count}")

    result = CopyResult(
        loader_name=config.name,
        table_name=table_name,
        source=source_table,
        dest=target_table,
        copy_columns=copy_columns,
        source_row_count=source_row_count,
        dest_row_count_before=dest_row_count,
        rows_copied=None,
        status="DRY_RUN",
    )

    # Write mode: by default insert only if the target prod table is empty.
    # With truncate=True, the target is emptied first and then filled regardless.
    if dest_row_count > 0 and not truncate:
        message = (
            f"Target {target_table.display_name()} already has {dest_row_count} rows; "
            "write mode is 'insert only if empty'. Skipping without writing. "
            "Pass --truncate to truncate and refill."
        )
        out(f"  {message}")
        result.status = "SKIPPED_NONEMPTY"
        result.message = message
        return result

    if dry_run:
        if truncate and dest_row_count > 0:
            out(f"  Dry run: would TRUNCATE {dest_row_count} rows, then write {source_row_count} rows.")
        else:
            out("  Dry run: no rows written.")
        result.status = "DRY_RUN"
        return result

    if not assume_yes:
        out("")
        if truncate:
            action = (
                f"About to TRUNCATE PROD table "
                f"{target_table.display_name(include_server=True)} ({dest_row_count} rows) "
                f"and copy {source_row_count} rows into it."
            )
        else:
            action = (
                f"About to copy {source_row_count} rows into PROD table "
                f"{target_table.display_name(include_server=True)}."
            )
        prompt = f"{action}\nType 'yes' to proceed: "
        try:
            answer = input(prompt)
        except EOFError:
            answer = ""
        if answer.strip().lower() != "yes":
            out("  Aborted; no rows written.")
            result.status = "ABORTED"
            result.message = "User did not confirm."
            return result

    out(f"  Reading source data in chunks of {chunk_size:,} rows...")
    chunks = read_table_chunks(source_table, copy_columns, chunk_size=chunk_size)
    dst_cnxn = connect_sql_server(target_table.server, target_table.database)
    try:
        # Fetch the first chunk before touching the target so a failing source
        # read cannot leave behind a truncated or partially filled target.
        first_chunk = next(chunks, None)

        current = count_rows(dst_cnxn, target_table)
        if truncate:
            if current > 0:
                out(f"  Truncating {current} rows from {target_table.display_name()}...")
                truncate_table(dst_cnxn, target_table)
                result.rows_truncated = current
        else:
            # Re-check emptiness right before writing in case the table was filled
            # between the first count and now.
            if current > 0:
                message = f"Target became non-empty ({current} rows) before write; aborting without writing."
                out(f"  {message}")
                result.status = "SKIPPED_NONEMPTY"
                result.message = message
                return result

        rows = 0
        if first_chunk is not None:
            for chunk in itertools.chain([first_chunk], chunks):
                rows += insert_dataframe(
                    dst_cnxn, target_table, chunk, copy_columns, batch_size=config.batch_size
                )
                out(f"  ... {rows:,}/{source_row_count:,} rows copied.")
    finally:
        chunks.close()
        dst_cnxn.close()

    out(f"  Inserted {rows} rows into {target_table.display_name(include_server=True)}.")
    result.rows_copied = rows
    result.status = "COPIED"
    return result


def _parse_args(argv: list[str]) -> tuple[argparse.Namespace, str]:
    parser = argparse.ArgumentParser(
        prog="data_fill_helper",
        description="Copy one destination's PROD table into another destination's PROD table for a loader config.",
        epilog="Example: data_fill_helper --inventory_location   (copies des1 prod -> des2 prod)",
    )
    parser.add_argument("--config", default="configs", help="Config directory or .yaml file (default: configs).")
    parser.add_argument("--loader", default=None, help="Loader/config name, e.g. inventory_location.")
    parser.add_argument(
        "--from", "--source", dest="source_name", default=None,
        help="Source destination name (default: first destination, e.g. des1).",
    )
    parser.add_argument(
        "--to", "--dest", dest="dest_name", default=None,
        help="Target destination name (default: second destination, e.g. des2).",
    )
    # Which prod table(s) to copy, for a loader whose promotion writes more than one
    # (extra tables declared under each destination's `prod.aux`). Mutually
    # exclusive; with neither, only the destination's own prod table is copied.
    tables = parser.add_mutually_exclusive_group()
    tables.add_argument(
        "--table", default=None,
        help=f"Prod table to copy: {MAIN_TABLE_KEY} (default, the destination's own prod table) "
             f"or a prod.aux name, e.g. stat.",
    )
    tables.add_argument(
        "--all-tables", action="store_true",
        help="Copy every prod table the loader declares (primary first, then each prod.aux), "
             "each with its own confirmation.",
    )
    parser.add_argument("--yes", "-y", action="store_true", help="Skip the confirmation prompt.")
    parser.add_argument("--dry-run", action="store_true", help="Read and report only; never write.")
    parser.add_argument(
        "--truncate", action="store_true",
        help="TRUNCATE the target prod table first, then fill it regardless of its current row count.",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
        help=f"Rows to stream from the source per chunk (default: {DEFAULT_CHUNK_SIZE}).",
    )

    args, extras = parser.parse_known_args(argv)

    # Allow the config name to be given as a bare token or as --<name>
    # (e.g. `--inventory_location`) in addition to `--loader <name>`.
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
        parser.error("no loader specified; pass the config name, e.g. --inventory_location")
    return args, loader_name


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    args, loader_name = _parse_args(argv)

    configs = load_loader_configs(args.config)
    loader = get_loader(configs, loader_name)

    results: list[CopyResult] = []
    try:
        table_names = (
            prod_table_names(loader, args.source_name, args.dest_name)
            if args.all_tables
            else [args.table or MAIN_TABLE_KEY]
        )
        for index, table_name in enumerate(table_names):
            if index:
                print()
            result = copy_prod_table(
                loader,
                source_name=args.source_name,
                dest_name=args.dest_name,
                table_name=table_name,
                dry_run=args.dry_run,
                assume_yes=args.yes,
                truncate=args.truncate,
                chunk_size=args.chunk_size,
            )
            results.append(result)
            # Declining one table's prompt stops the whole run: the remaining
            # tables are part of the same fill, so keep prod as it is.
            if result.status == "ABORTED":
                print("Aborted; remaining tables were not copied.", file=sys.stderr)
                break
    except Exception as exc:  # noqa: BLE001 - surface a clean message instead of a traceback.
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print()
    for result in results:
        print(
            f"{result.status}\t{result.loader_name}\t{result.table_name}\t"
            f"rows_truncated={result.rows_truncated}\trows_copied={result.rows_copied}"
        )
    return 0 if all(result.status != "ABORTED" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
