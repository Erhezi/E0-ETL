from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .config import LoaderConfig, TableRef
from .db import connect_sql_server, get_insert_columns, get_table_columns


def read_source_headers(loader_config: LoaderConfig) -> list[str]:
    source = loader_config.source_files[0]
    file_path = source.resolve()
    options = dict(source.options)
    if source.reader == "csv":
        options["nrows"] = 0
        columns = list(pd.read_csv(file_path, **options).columns)
    elif source.reader == "excel":
        options["nrows"] = 0
        columns = list(pd.read_excel(file_path, **options).columns)
    else:
        raise ValueError(f"Unsupported reader {source.reader!r}")

    renamed = [loader_config.rename_columns.get(column, column) for column in columns]
    drop_columns = set(loader_config.drop_columns)
    return [column for column in renamed if column not in drop_columns]


def inspect_table(table: TableRef) -> list[dict[str, Any]]:
    cnxn = connect_sql_server(table.server, table.database)
    try:
        return get_table_columns(cnxn, table)
    finally:
        cnxn.close()


def build_column_mapping_template(
    loader_config: LoaderConfig,
    *,
    include_source_headers: bool = True,
) -> dict[str, Any]:
    cnxn = connect_sql_server(loader_config.destination.server, loader_config.destination.database)
    try:
        db_columns = get_insert_columns(
            cnxn,
            loader_config.destination,
            skip_identity_columns=loader_config.skip_identity_columns,
        )
    finally:
        cnxn.close()

    source_headers = read_source_headers(loader_config) if include_source_headers else []
    source_set = set(source_headers)
    mapping = [
        {
            "destination": column,
            "source": column if column in source_set else None,
        }
        for column in db_columns
    ]
    mapped_sources = {item["source"] for item in mapping if item["source"] is not None}
    ignored_source_columns = [column for column in source_headers if column not in mapped_sources]
    first_destination = loader_config.destinations[0]
    return {
        "loader": loader_config.name,
        "destination": first_destination.display_name(include_server=True),
        "staging": first_destination.staging.display_name(include_server=True),
        "prod": first_destination.prod.display_name(include_server=True) if first_destination.prod else None,
        "column_mapping": mapping,
        "ignored_source_columns": ignored_source_columns,
    }


def write_mapping_template(loader_config: LoaderConfig, output_path: str | Path) -> Path:
    payload = build_column_mapping_template(loader_config)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output
