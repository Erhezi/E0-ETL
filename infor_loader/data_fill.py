"""Pre-populate one destination's PROD table from another destination's PROD table.

Given a loader config (e.g. ``inventory_location``), this reads rows from the
source destination's prod table and inserts them into the target destination's
prod table. Unlike the file loaders, the input here is an existing prod table,
not a CSV/Excel file -- it is meant for a one-time pre-population.

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
from .config import LoadDestination, LoaderConfig, TableRef
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


def copy_prod_table(
    config: LoaderConfig,
    *,
    source_name: str | None = None,
    dest_name: str | None = None,
    dry_run: bool = False,
    assume_yes: bool = False,
    truncate: bool = False,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    out: Callable[[str], None] = print,
) -> CopyResult:
    """Copy the source destination's prod table into the target destination's prod table.

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
    if source_dest.prod is None:
        raise ValueError(f"Source destination {source_dest.name!r} has no prod table configured.")
    if target_dest.prod is None:
        raise ValueError(f"Target destination {target_dest.name!r} has no prod table configured.")

    source_table = source_dest.prod
    target_table = target_dest.prod

    out(f"Loader: {config.name}")
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

    try:
        result = copy_prod_table(
            loader,
            source_name=args.source_name,
            dest_name=args.dest_name,
            dry_run=args.dry_run,
            assume_yes=args.yes,
            truncate=args.truncate,
            chunk_size=args.chunk_size,
        )
    except Exception as exc:  # noqa: BLE001 - surface a clean message instead of a traceback.
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(
        f"\n{result.status}\t{result.loader_name}\t"
        f"rows_truncated={result.rows_truncated}\trows_copied={result.rows_copied}"
    )
    return 0 if result.status != "ABORTED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
