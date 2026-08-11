from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"

# Key the PRIMARY prod table takes in LoadDestination.prod_tables; each `prod.aux`
# entry sits alongside it under its own name. Tools that address one production
# table by name (data_fill's --table) use this for "the destination's own prod table".
MAIN_TABLE_KEY = "main"

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
    # Auxiliary staging tables on THIS destination, keyed by the loader-level
    # aux_stagings entry's `name` (see AuxStaging). Declared per destination -- next
    # to `staging` under `staging.aux` -- so every staging table this destination
    # uses is listed in one place and the prod ETLHealth row can name them. Same
    # server/database/schema as `staging`.
    aux_staging: dict[str, TableRef] = field(default_factory=dict)
    # Additional PRODUCTION tables this destination's promotion writes, keyed by a
    # loader-chosen name (declared under `prod.aux`, mirroring `staging.aux`), with
    # the same server/database/schema as `prod` unless overridden. The prod.post_sql
    # statements own the writes; declaring the tables here is what lets the run log an
    # ETLHealth row for each one and lets data_fill copy them destination-to-
    # destination by name. Every enabled destination must declare the same names --
    # each name identifies the SAME logical table on both sides (see LoaderConfig).
    aux_prod: dict[str, TableRef] = field(default_factory=dict)
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

        # Additional staging tables listed under `staging.aux` (name -> table). They
        # inherit the staging block's own server/db/schema (which fall back to the
        # destination base), so build them from the staging block, not the raw base.
        aux_staging: dict[str, TableRef] = {}
        if isinstance(staging_value, dict):
            staging_value = dict(staging_value)
            aux_staging = _aux_table_refs(
                staging_value.pop("aux", None),
                _block_defaults(staging_value, base),
                index=index,
                block="staging",
            )

        prod_value = data.get("prod") or data.get("production") or data.get("prod_table") or data.get("target_table")
        prod_post_sql: tuple[str, ...] = ()
        # Same shape on the prod side: `prod.aux` lists the OTHER production tables
        # the promotion statements write, inheriting the prod block's own server/db/
        # schema. `post_sql`/`aux` are popped so what is left describes the table.
        aux_prod: dict[str, TableRef] = {}
        if isinstance(prod_value, dict):
            prod_value = dict(prod_value)
            prod_post_sql = tuple(prod_value.pop("post_sql", None) or ())
            aux_prod = _aux_table_refs(
                prod_value.pop("aux", None),
                _block_defaults(prod_value, base),
                index=index,
                block="prod",
            )
            if MAIN_TABLE_KEY in aux_prod:
                raise ValueError(
                    f"Destination {index + 1} cannot name a prod.aux table {MAIN_TABLE_KEY!r}: "
                    f"that name is reserved for the destination's own `prod.table`."
                )
        return cls(
            name=str(data.get("name") or data.get("alias") or f"destination_{index + 1}"),
            staging=_table_ref_from_value(staging_value, base),
            prod=_table_ref_from_value(prod_value, base) if prod_value is not None else None,
            aux_staging=aux_staging,
            aux_prod=aux_prod,
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

    @property
    def prod_tables(self) -> dict[str, TableRef]:
        """Every production table this destination owns, keyed by name and in
        declaration order: the primary one under :data:`MAIN_TABLE_KEY`, then each
        ``prod.aux`` entry. A direct destination has no separate prod table -- its
        single configured table IS production -- so that table is the ``main`` entry
        (the same ``prod or staging`` idiom the load path uses)."""
        return {MAIN_TABLE_KEY: self.prod or self.staging, **self.aux_prod}

    def display_name(self, include_server: bool = False) -> str:
        staging_name = self.staging.display_name(include_server=include_server)
        aux_suffix = _aux_display(self.aux_staging, "aux staging", include_server)
        if self.prod is None:
            return f"{self.name}: direct={staging_name}{aux_suffix}"
        prod_name = self.prod.display_name(include_server=include_server)
        prod_suffix = _aux_display(self.aux_prod, "aux prod", include_server)
        return f"{self.name}: staging={staging_name}{aux_suffix}, prod={prod_name}{prod_suffix}"


def _block_defaults(block: dict[str, Any], base: dict[str, Any]) -> dict[str, Any]:
    """server/database/schema an ``aux`` table inherits: the owning block's own
    overrides where it sets them, else the destination-level defaults. Used so a
    ``staging.aux``/``prod.aux`` table follows the block it is declared in even when
    that block overrides the destination (e.g. staging in a different database)."""
    return {
        key: block.get(key, base.get(key))
        for key in ["server", "database", "schema"]
        if block.get(key) is not None or base.get(key) is not None
    }


def _aux_table_refs(
    aux_raw: Any,
    defaults: dict[str, Any],
    *,
    index: int,
    block: str,
) -> dict[str, TableRef]:
    """Build one destination's ``<block>.aux`` mapping of name -> :class:`TableRef`."""
    if aux_raw is None:
        return {}
    if not isinstance(aux_raw, dict):
        raise ValueError(
            f"Destination {index + 1} {block}.aux must be a mapping of "
            f"name -> table; got {type(aux_raw).__name__}."
        )
    return {
        str(aux_name): _table_ref_from_value(table_value, defaults)
        for aux_name, table_value in aux_raw.items()
    }


def _aux_display(tables: dict[str, TableRef], label: str, include_server: bool) -> str:
    """`` (+aux staging: a, b)`` suffix for a destination summary; '' when empty."""
    if not tables:
        return ""
    names = ", ".join(ref.display_name(include_server=include_server) for ref in tables.values())
    return f" (+{label}: {names})"


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


@dataclass(frozen=True)
class AuxStaging:
    """A secondary source file loaded into its own staging table, in ADDITION to
    the primary file's staging load, on EACH enabled destination's connection.

    This is distinct from the extra source files a transform consumes (e.g.
    poline's company_map/fd3/fd5): those merge into the primary frame and never
    land in a table. An aux staging is prepared on its own -- type conversions +
    source->destination mapping + normalize -- and truncate/inserted into its
    per-destination table AFTER the primary staging load and BEFORE the prod
    promotion. It exists so a destination's prod.post_sql (e.g. the requisition
    PO-source merge) can read a purpose-built feed not carried by the main export.

    The staging TABLE is declared per destination (``staging.aux: {name: table}``),
    keyed by ``name`` here, so every staging table a destination uses is listed
    together in its own block. This entry carries only the loader-wide bits shared
    across destinations: which source file feeds it (``source_alias``) and the
    mapping/type rules (mirroring the loader-level ``field_config``, applied by
    FileLoader._prepare_aux_frames exactly as the primary pipeline applies its own).
    """

    name: str
    source_alias: str
    column_mapping: list[dict[str, Any]] = field(default_factory=list)
    type_changes: dict[str, list[str]] = field(default_factory=dict)
    datetime_to_date_columns: list[str] = field(default_factory=list)
    pk_check: list[str] = field(default_factory=list)
    date_format: str | None = None
    datetime_format: str | None = None
    rename_columns: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AuxStaging":
        source_alias = data.get("source_alias") or data.get("source")
        if not source_alias:
            raise ValueError("aux_stagings entry requires 'source' (a source-file alias).")
        # `name` keys this entry to its per-destination staging.aux table; it
        # defaults to the source alias, which is the natural identifier.
        name = data.get("name") or source_alias
        raw_types = data.get("type_changes") or {}
        return cls(
            name=str(name),
            source_alias=str(source_alias),
            column_mapping=list(data.get("column_mapping") or []),
            type_changes={
                "date": list(raw_types.get("date", [])),
                "datetime": list(raw_types.get("datetime", [])),
                "float": list(raw_types.get("float", [])),
                "int": list(raw_types.get("int", [])),
            },
            datetime_to_date_columns=list(data.get("datetime_to_date_columns") or []),
            pk_check=list(data.get("pk_check") or []),
            date_format=data.get("date_format"),
            datetime_format=data.get("datetime_format"),
            rename_columns=dict(data.get("rename_columns") or {}),
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
    # Secondary files loaded into their own staging tables on each destination,
    # before prod promotion (see AuxStaging). Empty for the usual single-file loader.
    aux_stagings: list[AuxStaging] = field(default_factory=list)
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
        aux_stagings = [AuxStaging.from_dict(dict(item)) for item in data.get("aux_stagings", [])]
        # Every enabled destination must declare a table for every aux staging under
        # its own `staging.aux`, so all of a destination's staging tables live in one
        # block and the prod ETLHealth row can name them. A missing declaration would
        # also mean a prod.post_sql that reads the aux table has nothing loaded.
        for destination in enabled_destinations:
            missing = [aux.name for aux in aux_stagings if aux.name not in destination.aux_staging]
            if missing:
                raise ValueError(
                    f"{name}: destination {destination.name!r} must declare a staging table for "
                    f"aux staging(s) {missing} under staging.aux; every enabled destination has to "
                    f"list all aux staging tables."
                )
        # A `prod.aux` name identifies the SAME logical production table on every
        # destination (des1's InforContractLineErrorStat and des2's
        # ContractLineErrorStat are both `stat`), which is what lets data_fill copy
        # one named table between destinations. Names that differ across
        # destinations would leave a table unaddressable, so require them to match.
        aux_prod_names = {
            destination.name: sorted(destination.aux_prod) for destination in enabled_destinations
        }
        if len({tuple(names) for names in aux_prod_names.values()}) > 1:
            raise ValueError(
                f"{name}: prod.aux names differ across enabled destinations ({aux_prod_names}); "
                f"every enabled destination must declare the same aux prod table names -- a name "
                f"is the same logical table on each side."
            )
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
            aux_stagings=aux_stagings,
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
