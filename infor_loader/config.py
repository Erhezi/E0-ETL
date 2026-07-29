from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"

# How staging tables are loaded from the source file.
STG_LOAD_STRATEGIES = frozenset({"truncate_insert"})

# How prod tables are promoted from the just-loaded staging table.
#   truncate_insert          - replace prod wholesale with staging
#   pk_merge                 - upsert staging into prod on the PK (MERGE)
#   pk_delete_insert         - delete prod rows matching staging on the PK, then insert
#   pk_delete_insert_on_header - delete prod rows matching staging on the HEADER-level key
#                              (a prefix of the PK, e.g. Company + PayablesInvoice), then
#                              insert all staging rows; whole headers are replaced so lines
#                              deleted at the source disappear from prod too -- needed when
#                              the export window is keyed on the header's update stamp and
#                              Infor FSM omits deleted lines from the export
#   versioned_delete_insert  - delete prod rows matching staging on the PK + version stamps
#                              (e.g. create/update stamp), then insert all staging; unchanged
#                              rows are carried forward and changed rows accumulate as history,
#                              with the report/observation stamp advancing to the latest frontier
#   filter_active            - staging holds the full file (loaded truncate_insert), but prod
#                              receives only the subset of staging rows matching a set of
#                              "active" WHERE predicates (e.g. open/non-closed contract lines);
#                              the filtered-out rows are never promoted to prod
PRD_LOAD_STRATEGIES = frozenset(
    {
        "truncate_insert",
        "pk_merge",
        "pk_delete_insert",
        "pk_delete_insert_on_header",
        "versioned_delete_insert",
        "filter_active",
    }
)


def bracket_identifier(value: str) -> str:
    return "[" + value.replace("]", "]]") + "]"


def apply_loader_name(path_value: str | None, name: str) -> str | None:
    """Substitute the ``<loader name>`` placeholder in a config path with ``name``.

    Lets a shared template such as
    ``\\\\host\\share\\DailyLoader\\<loader name>\\logs`` be reused across loaders.
    """
    if path_value is None:
        return None
    return path_value.replace("<loader name>", name).replace("<loader_name>", name)


def _normalize_prd_load_strategy(value: Any) -> str | None:
    if value is None:
        return None
    strategy = str(value).strip()
    if strategy not in PRD_LOAD_STRATEGIES:
        allowed = ", ".join(sorted(PRD_LOAD_STRATEGIES))
        raise ValueError(f"prd_load.strategy must be one of: {allowed}; got {strategy!r}.")
    return strategy


# What a validation check does: run and fail on violation, run and only warn, or skip entirely.
OVERLAP_MODES = frozenset({"enforce", "warn", "skip"})


def _overlap_mode_from(data: dict[str, Any]) -> str:
    """Resolve the overlap-check mode, accepting the legacy enabled/on_failure keys."""
    raw_mode = data.get("mode")
    if raw_mode is None:
        # Back-compat: derive mode from the older enabled + on_failure keys.
        if not bool(data.get("enabled", True)):
            return "skip"
        on_failure = str(data.get("on_failure", "fail")).strip().lower()
        if on_failure not in {"fail", "warn"}:
            raise ValueError(
                f"validation.overlap_check.on_failure must be 'fail' or 'warn', got {on_failure!r}."
            )
        return "warn" if on_failure == "warn" else "enforce"
    mode = str(raw_mode).strip().lower()
    if mode not in OVERLAP_MODES:
        allowed = ", ".join(sorted(OVERLAP_MODES))
        raise ValueError(f"validation.overlap_check.mode must be one of: {allowed}; got {mode!r}.")
    return mode


@dataclass(frozen=True)
class TableRef:
    server: str
    database: str
    schema: str
    table: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TableRef":
        return cls(
            server=data["server"],
            database=data["database"],
            schema=data["schema"],
            table=data["table"],
        )

    def qualified_name(self, include_database: bool = False) -> str:
        parts = [self.schema, self.table]
        if include_database:
            parts = [self.database, *parts]
        return ".".join(bracket_identifier(part) for part in parts)

    def display_name(self, include_server: bool = False) -> str:
        name = f"{self.database}.{self.schema}.{self.table}"
        return f"{self.server}.{name}" if include_server else name


@dataclass(frozen=True)
class LoadDestination:
    name: str
    staging: TableRef
    prod: TableRef | None = None
    # Statements run on the *staging* connection after the staging load.
    post_sql: tuple[str, ...] = ()
    # Statements (e.g. EXEC promotion procs) run on the *prod* connection after
    # staging succeeds, to update prod from the just-loaded staging data.
    prod_post_sql: tuple[str, ...] = ()
    # A disabled destination is skipped entirely (no staging load, no prod
    # promotion, no ETLHealth rows) so a single target can be paused in config
    # without deleting its block.
    enabled: bool = True

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, index: int = 0) -> "LoadDestination":
        base = {
            key: data[key]
            for key in ["server", "database", "schema"]
            if data.get(key) is not None
        }
        staging_value = (
            data.get("staging")
            or data.get("staging_table")
            or data.get("destination")
            or data.get("table")
        )
        if staging_value is None:
            raise ValueError(f"Destination {index + 1} must define staging/staging_table/table.")

        prod_value = data.get("prod") or data.get("production") or data.get("prod_table") or data.get("target_table")
        prod_post_sql: tuple[str, ...] = ()
        if isinstance(prod_value, dict):
            prod_post_sql = tuple(prod_value.get("post_sql") or ())
        return cls(
            name=str(data.get("name") or data.get("alias") or f"destination_{index + 1}"),
            staging=_table_ref_from_value(staging_value, base),
            prod=_table_ref_from_value(prod_value, base) if prod_value is not None else None,
            post_sql=tuple(data.get("post_sql") or ()),
            prod_post_sql=prod_post_sql,
            enabled=bool(data.get("enabled", True)),
        )

    @property
    def is_direct(self) -> bool:
        """No prod table configured: the single configured table IS the final
        (production) table, loaded directly with the staging strategy (typically
        truncate_insert) and no promotion step. Used by master-data (mdm)
        loaders that have no staging table."""
        return self.prod is None

    def display_name(self, include_server: bool = False) -> str:
        staging_name = self.staging.display_name(include_server=include_server)
        if self.prod is None:
            return f"{self.name}: direct={staging_name}"
        prod_name = self.prod.display_name(include_server=include_server)
        return f"{self.name}: staging={staging_name}, prod={prod_name}"


def _table_ref_from_value(value: Any, defaults: dict[str, Any]) -> TableRef:
    if isinstance(value, TableRef):
        return value
    if isinstance(value, str):
        data = {**defaults, "table": value}
    elif isinstance(value, dict):
        data = {**defaults, **value}
    else:
        raise ValueError(f"Unsupported table reference: {value!r}")
    return TableRef.from_dict(data)


@dataclass(frozen=True)
class SourceFile:
    path: str
    name: str
    alias: str = "main"
    reader: str = "csv"
    options: dict[str, Any] = field(default_factory=dict)
    pick_latest: bool = False
    # The file-folder registry key this file was expanded from (`input: <key>` in
    # the loader YAML), or None for a file given by explicit path/name. Lets the
    # run path look this file's download rule back up in the registry to gate the
    # loader on a fresh download. Just a string -- no coupling to file_folder.
    input_key: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceFile":
        return cls(
            path=data["path"],
            name=data["name"],
            alias=data.get("alias", "main"),
            reader=data.get("reader", "csv"),
            options=dict(data.get("options", {})),
            pick_latest=bool(data.get("pick_latest", False)),
            input_key=data.get("input_key"),
        )

    def resolve(self) -> Path:
        base = Path(self.path)
        if any(char in self.name for char in "*?["):
            matches = sorted(base.glob(self.name), key=lambda p: p.stat().st_mtime)
            if not matches:
                raise FileNotFoundError(f"No files matched {base / self.name}")
            return matches[-1] if self.pick_latest else matches[0]
        file_path = base / self.name
        if not file_path.exists():
            raise FileNotFoundError(str(file_path))
        return file_path


@dataclass(frozen=True)
class OverlapCheck:
    """Delta-overlap guard for rolling-window source files.

    When the source file carries only the last N days keyed on an "update stamp"
    (or any monotonically increasing column), this asserts the incoming window
    overlaps what is already stored, so no rows fall into the gap between runs::

        MIN(source[source_column])  <=  MAX(baseline_table[db_column])

    ``source_column`` is the column as it appears in the prepared source frame
    (post-rename, pre-destination-mapping); ``db_column`` is the same column's
    destination name, read from the ``baseline`` table (``prod`` or ``staging``).
    """

    source_column: str
    db_column: str
    baseline: str = "prod"
    mode: str = "enforce"          # enforce = fail on gap, warn = log on gap, skip = do not run
    skip_if_db_empty: bool = True

    @property
    def enabled(self) -> bool:
        return self.mode != "skip"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OverlapCheck":
        mode = _overlap_mode_from(data)
        source_column = data.get("source_column") or data.get("column")
        if mode != "skip" and not source_column:
            raise ValueError(
                "validation.overlap_check requires 'source_column' (or 'column') unless mode is 'skip'."
            )
        db_column = data.get("db_column") or source_column
        baseline = str(data.get("baseline", "prod")).strip().lower()
        if baseline not in {"staging", "prod"}:
            raise ValueError(
                f"validation.overlap_check.baseline must be 'staging' or 'prod', got {baseline!r}."
            )
        return cls(
            source_column=str(source_column or ""),
            db_column=str(db_column or ""),
            baseline=baseline,
            mode=mode,
            skip_if_db_empty=bool(data.get("skip_if_db_empty", True)),
        )


@dataclass
class LoaderConfig:
    name: str
    process_name: str
    source_files: list[SourceFile]
    destination: TableRef
    destinations: list[LoadDestination] = field(default_factory=list)
    target_table: TableRef | None = None
    health_table: TableRef | None = None
    process_frequency: str = "Daily"
    stg_load_strategy: str = "truncate_insert"
    prd_load_strategy: str | None = None
    file_config: dict[str, Any] = field(default_factory=dict)
    transforms: list[str] = field(default_factory=list)
    transform_options: dict[str, Any] = field(default_factory=dict)
    pre_file_moves: list[dict[str, Any]] = field(default_factory=list)
    post_file_moves: list[dict[str, Any]] = field(default_factory=list)
    post_sql: list[str] = field(default_factory=list)
    batch_size: int = 50_000
    log_root: str = "logs"
    # Directory the source file is copied into after a successful load (with a
    # YYYYMMDD stamp appended to the name). None disables archiving.
    archive_dir: str | None = None
    log_level: str = "INFO"
    log_to_console: bool = True
    capture_streams: bool = True
    skip_identity_columns: bool = False
    use_db_column_order: bool = True
    allow_missing_destination_columns: bool = False
    fail_on_pk_duplicates: bool = True
    # Source column names normalize_for_db must NOT whitespace-trim before the
    # insert. For destinations whose primary key relies on significant LEADING
    # whitespace (SQL Server ignores trailing spaces in varchar key comparisons
    # but a leading space is a distinct key), trimming would collapse such rows
    # into duplicate keys. See mdm_vendor_item.
    preserve_whitespace_columns: list[str] = field(default_factory=list)
    overlap_check: OverlapCheck | None = None
    enabled: bool = True
    tags: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        health_table: TableRef | None = None,
    ) -> "LoaderConfig":
        logging_config = dict(data.get("logging", {}))
        destinations = _load_destinations_from_dict(data)
        enabled_destinations = [destination for destination in destinations if destination.enabled]
        if not enabled_destinations:
            raise ValueError(
                f"{data.get('name', '<unnamed loader>')}: every destination is disabled; "
                "enable at least one or disable the loader itself."
            )
        # The legacy single-destination fields (destination/target_table) and the
        # overlap-check baseline follow the first destination that will actually load.
        first_destination = enabled_destinations[0]
        overlap_raw = dict(data.get("validation") or {}).get("overlap_check")
        overlap_check = OverlapCheck.from_dict(dict(overlap_raw)) if overlap_raw else None
        name = data["name"]
        archive_dir = _archive_dir_from(data.get("archive"))
        return cls(
            name=name,
            process_name=data.get("process_name", data["name"]),
            source_files=[SourceFile.from_dict(item) for item in data["source_files"]],
            destination=first_destination.staging,
            destinations=destinations,
            target_table=(
                TableRef.from_dict(data["target_table"])
                if data.get("target_table")
                else first_destination.prod
            ),
            health_table=TableRef.from_dict(data["health_table"]) if data.get("health_table") else health_table,
            process_frequency=data.get("process_frequency", "Daily"),
            stg_load_strategy=data.get("stg_load_strategy", data.get("load_strategy", "truncate_insert")),
            prd_load_strategy=_normalize_prd_load_strategy(data.get("prd_load_strategy")),
            file_config=dict(data.get("file_config", {})),
            transforms=list(data.get("transforms", [])),
            transform_options=dict(data.get("transform_options", {})),
            pre_file_moves=list(data.get("pre_file_moves", [])),
            post_file_moves=list(data.get("post_file_moves", [])),
            post_sql=list(data.get("post_sql", [])),
            batch_size=int(data.get("batch_size", 50_000)),
            log_root=apply_loader_name(data.get("log_root") or logging_config.get("log_root", "logs"), name),
            archive_dir=apply_loader_name(archive_dir, name),
            log_level=str(logging_config.get("level", data.get("log_level", "INFO"))),
            log_to_console=bool(logging_config.get("console", data.get("log_to_console", True))),
            capture_streams=bool(logging_config.get("capture_streams", data.get("capture_streams", True))),
            skip_identity_columns=bool(data.get("skip_identity_columns", False)),
            use_db_column_order=bool(data.get("use_db_column_order", True)),
            allow_missing_destination_columns=bool(data.get("allow_missing_destination_columns", False)),
            fail_on_pk_duplicates=bool(data.get("fail_on_pk_duplicates", True)),
            preserve_whitespace_columns=[str(column) for column in (data.get("preserve_whitespace_columns") or [])],
            overlap_check=overlap_check,
            enabled=bool(data.get("enabled", True)),
            tags=list(data.get("tags", [])),
        )

    @property
    def multiple_input_files(self) -> bool:
        return len(self.source_files) > 1

    @property
    def rename_columns(self) -> dict[str, str]:
        return dict(self.file_config.get("rename_columns") or self.file_config.get("rename_cols") or {})

    @property
    def drop_columns(self) -> list[str]:
        return list(self.file_config.get("drop_columns") or self.file_config.get("drop_cols") or [])

    @property
    def type_changes(self) -> dict[str, list[str]]:
        raw = self.file_config.get("type_changes", {})
        return {
            "date": list(raw.get("date", [])),
            "datetime": list(raw.get("datetime", [])),
            "float": list(raw.get("float", [])),
            "int": list(raw.get("int", [])),
        }

    @property
    def date_format(self) -> str | None:
        """strptime format for `date`-typed source columns (None = infer)."""
        return self.file_config.get("date_format")

    @property
    def datetime_format(self) -> str | None:
        """strptime format for `datetime`-typed source columns (None = infer)."""
        return self.file_config.get("datetime_format")

    @property
    def datetime_to_date_columns(self) -> list[str]:
        """Columns typed `datetime` in the mapping whose destination column is
        `date`: they carry a time-of-day in the source (so a strict date parse
        would coerce them to the fallback) but must land as plain dates."""
        columns: list[str] = []
        for row in self.column_mapping or []:
            if str(row.get("type") or "").strip().lower() != "datetime":
                continue
            if str(row.get("destination_type") or "").strip().lower() != "date":
                continue
            column = row.get("source") or row.get("destination")
            if column:
                columns.append(column)
        return columns

    @property
    def destination_columns(self) -> list[str] | None:
        columns = self.file_config.get("destination_columns")
        if columns is None:
            columns = self.file_config.get("rearrange_cols")
        return list(columns) if columns else None

    @property
    def pk_check(self) -> list[str]:
        return list(self.file_config.get("pk_check") or [])

    @property
    def column_mapping(self) -> list[dict[str, Any]] | None:
        mapping = self.file_config.get("column_mapping")
        return list(mapping) if mapping else None

    @property
    def package_path(self) -> str:
        return str(Path.cwd())


def _archive_dir_from(archive: Any) -> str | None:
    """Read the archive directory from an ``archive`` config block.

    Accepts a plain string path, or a mapping with ``path`` (or ``dir``) and an
    optional ``enabled`` flag (default True). Returns None when archiving is off.
    """
    if archive is None:
        return None
    if isinstance(archive, str):
        return archive or None
    if isinstance(archive, dict):
        if not bool(archive.get("enabled", True)):
            return None
        return archive.get("path") or archive.get("dir") or None
    raise ValueError(f"archive must be a string or mapping; got {type(archive).__name__}.")


def _load_destinations_from_dict(data: dict[str, Any]) -> list[LoadDestination]:
    raw_destinations = data.get("destinations")
    if raw_destinations:
        if not isinstance(raw_destinations, list):
            raise ValueError("destinations must be a list.")
        return [
            LoadDestination.from_dict(dict(destination), index=index)
            for index, destination in enumerate(raw_destinations)
        ]

    if not data.get("destination"):
        raise ValueError("Loader config must define destination or destinations.")

    raw_destination = data["destination"]
    if isinstance(raw_destination, list):
        return [
            LoadDestination.from_dict(dict(destination), index=index)
            for index, destination in enumerate(raw_destination)
        ]

    staging = TableRef.from_dict(raw_destination)
    prod = TableRef.from_dict(data["target_table"]) if data.get("target_table") else None
    return [LoadDestination(name="default", staging=staging, prod=prod)]
