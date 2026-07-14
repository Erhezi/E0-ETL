from __future__ import annotations

import datetime as dt
import math
from typing import Any, Iterable, Iterator
from urllib.parse import quote_plus

import pandas as pd

from .config import DEFAULT_DRIVER, TableRef, bracket_identifier


def connect_sql_server(server: str, database: str, *, driver: str = DEFAULT_DRIVER):
    import pyodbc

    return pyodbc.connect(
        driver="{" + driver + "}",
        server=server,
        database=database,
        trusted_connection="yes",
    )


def create_sqlalchemy_engine(server: str, database: str, *, driver: str = DEFAULT_DRIVER):
    from sqlalchemy import create_engine

    odbc_connect = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_connect)}")


def get_table_columns(cnxn: Any, table: TableRef) -> list[dict[str, Any]]:
    sql = """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE,
            COLUMNPROPERTY(
                OBJECT_ID(QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)),
                COLUMN_NAME,
                'IsIdentity'
            ) AS IS_IDENTITY,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """
    cursor = cnxn.cursor()
    rows = cursor.execute(sql, table.schema, table.table).fetchall()
    if rows:
        return [
            {
                "name": row.COLUMN_NAME,
                "data_type": row.DATA_TYPE,
                "max_length": row.CHARACTER_MAXIMUM_LENGTH,
                "numeric_precision": row.NUMERIC_PRECISION,
                "numeric_scale": row.NUMERIC_SCALE,
                "is_nullable": row.IS_NULLABLE,
                "is_identity": bool(row.IS_IDENTITY),
                "ordinal_position": row.ORDINAL_POSITION,
            }
            for row in rows
        ]

    # Fallback for unusual schemas/table names where INFORMATION_SCHEMA is blocked.
    cursor.execute(f"SELECT TOP 0 * FROM {table.qualified_name()}")
    return [
        {
            "name": column[0],
            "data_type": None,
            "max_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": None,
            "is_identity": False,
            "ordinal_position": index + 1,
        }
        for index, column in enumerate(cursor.description or [])
    ]


def get_insert_columns(cnxn: Any, table: TableRef, *, skip_identity_columns: bool = False) -> list[str]:
    columns = get_table_columns(cnxn, table)
    if skip_identity_columns:
        columns = [column for column in columns if not column.get("is_identity")]
    return [column["name"] for column in columns]


def truncate_table(cnxn: Any, table: TableRef) -> None:
    cursor = cnxn.cursor()
    cursor.execute(f"TRUNCATE TABLE {table.qualified_name()}")
    cnxn.commit()


def get_max_value(cnxn: Any, table: TableRef, column: str) -> Any:
    """Return ``MAX(column)`` from ``table``, or None when the table has no rows
    (or the column is all-NULL). Used by the delta-overlap validation."""
    cursor = cnxn.cursor()
    sql = f"SELECT MAX({bracket_identifier(column)}) FROM {table.qualified_name()}"
    row = cursor.execute(sql).fetchone()
    cursor.close()
    return row[0] if row is not None else None


def count_rows(cnxn: Any, table: TableRef) -> int:
    cursor = cnxn.cursor()
    count = cursor.execute(f"SELECT COUNT(*) FROM {table.qualified_name()}").fetchone()[0]
    cursor.close()
    return int(count)


def read_table(
    table: TableRef,
    columns: list[str] | None = None,
    *,
    driver: str = DEFAULT_DRIVER,
) -> pd.DataFrame:
    """Read a SQL Server table into a DataFrame, selecting ``columns`` if given."""
    engine = create_sqlalchemy_engine(table.server, table.database, driver=driver)
    try:
        column_sql = ",".join(bracket_identifier(column) for column in columns) if columns else "*"
        return pd.read_sql_query(f"SELECT {column_sql} FROM {table.qualified_name()}", engine)
    finally:
        engine.dispose()


def read_table_chunks(
    table: TableRef,
    columns: list[str] | None = None,
    *,
    chunk_size: int = 500_000,
    driver: str = DEFAULT_DRIVER,
) -> Iterator[pd.DataFrame]:
    """Stream a SQL Server table as DataFrame chunks of up to ``chunk_size`` rows.

    The query executes once; rows are fetched lazily as the iterator is consumed,
    so at most one chunk is held in memory at a time. The engine is disposed when
    the iterator is exhausted or closed.
    """
    engine = create_sqlalchemy_engine(table.server, table.database, driver=driver)
    try:
        column_sql = ",".join(bracket_identifier(column) for column in columns) if columns else "*"
        yield from pd.read_sql_query(
            f"SELECT {column_sql} FROM {table.qualified_name()}", engine, chunksize=chunk_size
        )
    finally:
        engine.dispose()


def execute_statements(cnxn: Any, statements: Iterable[str]) -> None:
    cursor = cnxn.cursor()
    for statement in statements:
        cursor.execute(statement)
    cnxn.commit()


def insert_dataframe(
    cnxn: Any,
    table: TableRef,
    df: pd.DataFrame,
    columns: list[str],
    *,
    batch_size: int = 50_000,
) -> int:
    if not columns:
        raise ValueError(f"No insert columns resolved for {table.display_name()}")

    insert_columns = ",".join(bracket_identifier(column) for column in columns)
    placeholders = ",".join("?" for _ in columns)
    insert_sql = f"INSERT INTO {table.qualified_name()} ({insert_columns}) VALUES ({placeholders})"

    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    row_count = 0
    for start in range(0, len(df), batch_size):
        chunk = df.iloc[start : start + batch_size]
        values = [_clean_row(row) for row in chunk[columns].itertuples(index=False, name=None)]
        if values:
            cursor.executemany(insert_sql, values)
            cnxn.commit()
            row_count += len(values)
    cursor.close()
    return row_count


def insert_health_record(cnxn: Any, table: TableRef, payload: dict[str, Any]) -> None:
    columns = [
        "ProcessName",
        "ProcessID",
        "SourceFilePath",
        "LastRunTime",
        "TargetTableName",
        "TaskStatus",
        "RowCount",
        "PackagePath",
        "LogFilePath",
        "STGTableName",
        "ProcessFrequency",
        "Error",
        "Duration",
        "DBConnection",
        "ProcessType",
        "TargetTableType",
    ]
    insert_columns = ",".join(bracket_identifier(column) for column in columns)
    placeholders = ",".join("?" for _ in columns)
    sql = f"INSERT INTO {table.qualified_name()} ({insert_columns}) VALUES ({placeholders})"
    values = [_clean_value(payload.get(column)) for column in columns]
    cursor = cnxn.cursor()
    cursor.execute(sql, *values)
    cnxn.commit()
    cursor.close()


def _clean_row(row: tuple[Any, ...]) -> tuple[Any, ...]:
    return tuple(_clean_value(value) for value in row)


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if pd.isna(value) and not isinstance(value, (str, bytes)):
        return None
    if isinstance(value, pd.Timestamp):
        if value is pd.NaT:
            return None
        return value.to_pydatetime()
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    return value
