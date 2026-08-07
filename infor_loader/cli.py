from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .config import LoaderConfig, TableRef
from .file_folder import (
    CONFIG_FILENAMES as FILE_FOLDER_CONFIG_FILENAMES,
    DEFAULT_CONFIG_PATH as FILE_FOLDER_DEFAULT_CONFIG,
    ON_DEMAND_TAG,
    InputFile,
    InputRegistry,
    load_input_registry,
    run_moves,
    select_inputs,
)
from .file_loader import FileLoader, LoaderResult
from .utilities import build_column_mapping_template, inspect_table, write_mapping_template


# Upper bound on concurrent loaders so a full run cannot swamp the SQL Server.
MAX_WORKERS_CAP = 4

# ON_DEMAND_TAG (imported from file_folder) gates on-demand loaders: they are
# discovered (so name selection and `list` work) but excluded from the `--all`
# batch and the `move-files --check` source scan -- run them explicitly by name
# (`--loader <name>`). By convention these live under `configs/loaders/on-demand/`;
# the tag, not the folder, is what gates the behavior.


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run configurable Infor file loaders.")
    parser.add_argument("--config", default="configs", help="Config directory, .yaml file, Python module, or .py path.")

    # Top-level run shortcut: `run_daily_loaders.py --loader <name>` prints a
    # summary and asks for confirmation before running; `--auto` skips the prompt.
    parser.add_argument("--loader", action="append", default=[], help="Loader name to run. Can be repeated.")
    parser.add_argument("--tag", action="append", default=[], help="Select loaders by tag. Can be repeated.")
    parser.add_argument("--all", action="store_true", help="Run every daily loader (excludes on-demand loaders).")
    parser.add_argument("--auto", action="store_true", help="Skip the confirmation prompt and run immediately.")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help=f"Parallel worker count, capped at {MAX_WORKERS_CAP}. Defaults to {MAX_WORKERS_CAP} with --all, else 1.",
    )
    parser.add_argument("--log-root", default=None, help="Override each loader's YAML logging.log_root.")
    parser.add_argument(
        "--ignore-download",
        action="store_true",
        help="Skip the Downloads check and load the file already in each loader's designated "
        "source folder (no fresh download required). Use for backfills / reprocessing.",
    )
    _add_phase_flags(parser)

    subparsers = parser.add_subparsers(dest="command", required=False)

    list_parser = subparsers.add_parser("list", help="List configured loaders.")
    list_parser.add_argument("--tag", action="append", default=[])

    run_parser = subparsers.add_parser("run", help="Run configured loaders.")
    run_parser.add_argument("--loader", action="append", default=[], help="Loader name to run. Can be repeated.")
    run_parser.add_argument("--tag", action="append", default=[], help="Run loaders with tag. Can be repeated.")
    run_parser.add_argument("--all", action="store_true", help="Run every daily loader (excludes on-demand loaders).")
    run_parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help=f"Parallel worker count, capped at {MAX_WORKERS_CAP}. Defaults to {MAX_WORKERS_CAP} with --all, else 1.",
    )
    run_parser.add_argument("--log-root", default=None, help="Override each loader's YAML logging.log_root.")
    run_parser.add_argument(
        "--ignore-download",
        action="store_true",
        help="Skip the Downloads check and load the file already in each loader's designated "
        "source folder (no fresh download required). Use for backfills / reprocessing.",
    )
    _add_phase_flags(run_parser)

    files_parser = subparsers.add_parser(
        "move-files",
        help="Move downloaded export files into each loader's designated source folder.",
    )
    files_parser.add_argument(
        "--file-config",
        default=FILE_FOLDER_DEFAULT_CONFIG,
        help=f"File-folder registry YAML. Defaults to {FILE_FOLDER_DEFAULT_CONFIG}.",
    )
    files_parser.add_argument("--input", action="append", default=[], dest="inputs",
                              help="Input key to dispatch (e.g. item, poline). Can be repeated.")
    files_parser.add_argument("--tag", action="append", default=[], help="Select inputs by tag. Can be repeated.")
    files_parser.add_argument("--dry-run", action="store_true", help="Report what would be moved without touching any file.")
    files_parser.add_argument("--list", action="store_true", dest="list_moves", help="List configured download inputs and exit.")
    files_parser.add_argument(
        "--check",
        action="store_true",
        help="After moving, verify the fed loaders' source files resolve (all enabled loaders when no rule names any).",
    )

    mapping_parser = subparsers.add_parser("mapping-template", help="Generate a destination-column mapping template.")
    mapping_parser.add_argument("--loader", required=True)
    mapping_parser.add_argument("--output", required=True)

    table_parser = subparsers.add_parser("table-info", help="Print SQL Server destination table columns.")
    table_parser.add_argument("--server", required=True)
    table_parser.add_argument("--database", required=True)
    table_parser.add_argument("--schema", required=True)
    table_parser.add_argument("--table", required=True)

    args = parser.parse_args(argv)

    if getattr(args, "all", False) and (args.loader or args.tag):
        parser.error("--all cannot be combined with --loader/--tag.")

    if args.command is None:
        if args.loader or args.tag or args.all:
            return run_interactive(
                args.config,
                names=args.loader,
                tags=args.tag,
                select_all=args.all,
                auto=args.auto,
                max_workers=_resolve_max_workers(args),
                log_root=args.log_root,
                phase=_resolve_phase(args),
                ignore_download=args.ignore_download,
            )
        parser.print_help(sys.stderr)
        return 2

    if args.command == "table-info":
        table = TableRef(args.server, args.database, args.schema, args.table)
        print(json.dumps(inspect_table(table), indent=2, default=str))
        return 0

    if args.command == "move-files":
        return run_move_files(
            args.file_config,
            names=args.inputs,
            tags=args.tag,
            dry_run=args.dry_run,
            list_only=args.list_moves,
            check=args.check,
            loader_config_ref=args.config,
        )

    loaders = load_loader_configs(args.config)
    if args.command == "list":
        for loader in select_loaders(loaders, tags=args.tag):
            enabled = "enabled" if loader.enabled else "disabled"
            destinations = "; ".join(
                destination.display_name(include_server=True)
                for destination in loader.destinations
            )
            print(f"{loader.name}\t{enabled}\t{','.join(loader.tags)}\t{destinations}")
        return 0

    if args.command == "mapping-template":
        loader = get_loader(loaders, args.loader)
        output = write_mapping_template(loader, args.output)
        print(output)
        return 0

    if args.command == "run":
        selected = _daily_loaders(loaders) if args.all else select_loaders(loaders, names=args.loader, tags=args.tag)
        selected = [loader for loader in selected if loader.enabled]
        if not selected:
            print("No enabled loaders selected.", file=sys.stderr)
            return 2
        results = run_loaders(
            selected,
            max_workers=_resolve_max_workers(args),
            log_root=args.log_root,
            phase=_resolve_phase(args),
            registry=_find_registry_for(args.config),
            ignore_download=args.ignore_download,
        )
        for result in results:
            row_text = "" if result.row_count is None else f" rows={result.row_count}"
            print(f"{result.status}\t{result.loader_name}\t{result.duration_seconds}s{row_text}\t{result.log_file_path}")
        # A FILE_NOT_FOUND skip (no fresh download) is a benign, expected outcome,
        # not a failure: it must not turn the whole run's exit code non-zero.
        return 0 if all(result.status in {"SUCCESS", "FILE_NOT_FOUND"} for result in results) else 1

    raise ValueError(f"Unknown command: {args.command}")


def load_loader_configs(config_ref: str) -> list[LoaderConfig]:
    config_path = Path(config_ref)
    if config_path.exists():
        if config_path.is_dir():
            # Loader YAMLs live in the `loaders/` subfolder when it exists
            # (configs/loaders/); the file-folder registry stays at the configs
            # root. The loaders may be grouped into subfolders (daily/, on-demand/),
            # so recurse to discover every group. Fall back to the directory itself
            # (non-recursive) for the older flat layout. The registry is still
            # skipped by name if it sits alongside.
            registry = _find_input_registry(config_path)
            loader_dir = config_path / "loaders"
            if loader_dir.is_dir():
                candidates = [*loader_dir.rglob("*.yaml"), *loader_dir.rglob("*.yml")]
            else:
                loader_dir = config_path
                candidates = [*loader_dir.glob("*.yaml"), *loader_dir.glob("*.yml")]
            yaml_paths = sorted(
                path
                for path in candidates
                if path.name.lower() not in FILE_FOLDER_CONFIG_FILENAMES
            )
            return [_load_yaml_config(path, registry) for path in yaml_paths]
        if config_path.suffix.lower() in {".yaml", ".yml"}:
            registry = _find_input_registry(config_path.parent)
            return [_load_yaml_config(config_path, registry)]

    module = _load_config_module(config_ref)
    health_table = TableRef.from_dict(module.HEALTH_TABLE) if hasattr(module, "HEALTH_TABLE") else None
    raw_loaders: list[dict[str, Any]]
    if hasattr(module, "get_loaders"):
        raw_loaders = module.get_loaders()
    else:
        raw_loaders = module.LOADERS
    return [LoaderConfig.from_dict(raw, health_table=health_table) for raw in raw_loaders]


def _find_input_registry(start: Path) -> InputRegistry | None:
    """Locate and load the file-folder registry, searching ``start`` then each
    parent directory. This lets the loaders sit in ``configs/loaders/`` while the
    registry stays at ``configs/`` -- the loaders dir is checked first, then its
    parent (the configs root) is found.

    Absent registry is fine for the older flat layout with inline path/name; a
    present-but-broken one raises so a bad registry fails loudly and early rather
    than resolving loader inputs to the wrong place."""
    for directory in [start, *start.parents]:
        for filename in sorted(FILE_FOLDER_CONFIG_FILENAMES):
            candidate = directory / filename
            if candidate.exists():
                return load_input_registry(candidate)
    return None


def _find_registry_for(config_ref: str) -> InputRegistry | None:
    """Load the file-folder registry for a config reference, mirroring how
    ``load_loader_configs`` locates it: a config directory searches itself and
    its parents, a single loader YAML searches its parent. Python-module configs
    (and any path with no registry nearby) yield None -- gating is then a no-op."""
    config_path = Path(config_ref)
    if config_path.is_dir():
        return _find_input_registry(config_path)
    if config_path.suffix.lower() in {".yaml", ".yml"}:
        return _find_input_registry(config_path.parent)
    return None


def _resolve_download_inputs(loader: LoaderConfig, registry: InputRegistry | None) -> list[InputFile]:
    """The loader's inputs that arrive as downloads (have a download block), in
    source-file order. These are what the run-time download gate checks; a file
    given by explicit path/name (no ``input_key``) or a fixture (no download
    block, e.g. company_map) contributes nothing and never gates the loader."""
    if registry is None:
        return []
    inputs: list[InputFile] = []
    for source_file in loader.source_files:
        if not source_file.input_key:
            continue
        input_file = registry.resolve(source_file.input_key)
        if input_file.download is not None:
            inputs.append(input_file)
    return inputs


def _load_yaml_config(path: Path, registry: InputRegistry | None = None) -> LoaderConfig:
    import yaml

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"YAML config must contain a mapping: {path}")
    raw = _normalize_yaml_config(data, registry)
    return LoaderConfig.from_dict(raw)


def _normalize_yaml_config(data: dict[str, Any], registry: InputRegistry | None = None) -> dict[str, Any]:
    raw = dict(data)
    connection = dict(raw.get("connection", {}))

    if "source" in raw and "source_files" not in raw:
        source = dict(raw["source"])
        files = source.get("files")
        if files is None:
            files = [source]
        source_files = []
        for file_config in files:
            item = dict(file_config)
            # `input: <key>` expands to the file's designated path + canonical
            # name from the central registry (configs/file_folder_loader_config
            # .yaml), so the location is defined in exactly one place. An
            # explicit path/name still works and wins if both are given.
            input_key = item.pop("input", None)
            if input_key is not None:
                if registry is None:
                    raise ValueError(
                        f"{raw.get('name', '<unnamed loader>')}: source file references input "
                        f"{input_key!r} but no file-folder registry was found in the configs directory or its parents."
                    )
                input_file = registry.resolve(input_key)
                item.setdefault("path", input_file.path)
                item.setdefault("name", input_file.name)
                # Remember the registry key so the run path can look this file's
                # download rule back up and gate the loader on a fresh download.
                item["input_key"] = input_key
            item.setdefault("path", source.get("path"))
            item.setdefault("reader", source.get("reader", "csv"))
            item.setdefault("options", source.get("options", {}))
            source_files.append(item)
        raw["source_files"] = source_files

    if isinstance(raw.get("destination"), list) and "destinations" not in raw:
        raw["destinations"] = raw.pop("destination")

    if raw.get("destinations"):
        raw["destinations"] = [
            _normalize_destination_config(item, connection, index=index)
            for index, item in enumerate(raw["destinations"])
        ]
    else:
        for key in ["destination", "target_table", "health_table"]:
            if raw.get(key):
                table = dict(raw[key])
                table.setdefault("server", connection.get("server"))
                table.setdefault("database", connection.get("database"))
                raw[key] = table

    if raw.get("health_table"):
        table = dict(raw["health_table"])
        table.setdefault("server", connection.get("server"))
        table.setdefault("database", connection.get("database"))
        raw["health_table"] = table

    if "field_config" in raw and "file_config" not in raw:
        field_config = dict(raw["field_config"])
        mapping = [_normalize_mapping_row(item) for item in list(field_config.get("mapping") or [])]
        # Type conversions come from the *loaded* mapping only; the not-loaded rows
        # appended below are type 'ignore' and contribute nothing.
        conversions = field_config.get("type_conversions") or field_config.get("type_changes") or _type_conversions_from_mapping(mapping)
        ignored_source_columns = _source_column_names(
            field_config.get("ignored_source_columns") or field_config.get("drop_columns") or []
        )
        # Source columns a transform consumes but that are not loaded to a destination.
        # Required to be present: a missing one is a hard failure, like a loaded column.
        for name in _source_column_names(field_config.get("transform_inputs") or []):
            mapping.append(_not_loaded_row(name, required_input=True))
        # Columns present in the file but not integrated into the pipeline yet.
        # Optional: a missing one only warns (it cannot affect the output).
        for name in _source_column_names(field_config.get("extra") or []):
            mapping.append(_not_loaded_row(name, required_input=False))
        raw["file_config"] = {
            "rename_columns": field_config.get("rename_columns"),
            "drop_columns": ignored_source_columns,
            "type_changes": {
                "date": list(conversions.get("date", [])),
                "datetime": list(conversions.get("datetime", [])),
                "float": list(conversions.get("float", [])),
                "int": list(conversions.get("int", [])),
            },
            "column_mapping": mapping,
            "pk_check": field_config.get("pk_check") or [],
            "date_format": field_config.get("date_format"),
            "datetime_format": field_config.get("datetime_format"),
        }

    # Aux staging blocks: each names a source-file alias + a staging table and
    # carries its OWN field_config (mapping/pk/date formats), normalized exactly
    # like the loader-level field_config above so the run-time prep applies the
    # same rules. Kept as a separate list so the primary pipeline is untouched.
    if raw.get("aux_stagings"):
        raw["aux_stagings"] = [
            _normalize_aux_staging(dict(entry), index=index)
            for index, entry in enumerate(raw["aux_stagings"])
        ]

    process = raw.get("process")
    if isinstance(process, dict):
        if process.get("name") is not None:
            raw.setdefault("process_name", process.get("name"))
        if process.get("frequency") is not None:
            raw.setdefault("process_frequency", process.get("frequency"))

    stg_load = raw.get("stg_load")
    if stg_load is None and isinstance(raw.get("load"), dict):
        stg_load = raw.get("load")  # back-compat: staging load block used to be `load`
    # `direct_load` is the preferred spelling of this same block for DIRECT
    # loaders (every destination is `table:` only -- no staging/prod split, the
    # final table is truncate+inserted in place). It feeds the same
    # stg_load_strategy/batch_size fields internally; the alias exists so the
    # config reads as what actually happens. Guarded so a staged loader cannot
    # hide its staging settings under the wrong name.
    if isinstance(raw.get("direct_load"), dict):
        loader_name = raw.get("name", "<unnamed loader>")
        if stg_load is not None:
            raise ValueError(f"{loader_name}: use direct_load or stg_load, not both.")
        prod_keys = ("prod", "production", "prod_table", "target_table")
        staged = [
            destination.get("name", f"destinations[{index}]")
            for index, destination in enumerate(raw.get("destinations") or [])
            if isinstance(destination, dict)
            and any(isinstance(destination.get(key), dict) for key in prod_keys)
        ]
        if staged:
            raise ValueError(
                f"{loader_name}: direct_load is only for direct destinations (table: only); "
                f"these have a staging/prod split: {staged}. Use stg_load."
            )
        stg_load = raw.get("direct_load")
    if isinstance(stg_load, dict):
        if stg_load.get("strategy") is not None:
            raw.setdefault("stg_load_strategy", stg_load.get("strategy"))
        if stg_load.get("batch_size") is not None:
            raw.setdefault("batch_size", stg_load.get("batch_size"))
        if stg_load.get("post_sql") is not None:
            raw.setdefault("post_sql", stg_load.get("post_sql", []))

    prd_load = raw.get("prd_load")
    if isinstance(prd_load, dict) and prd_load.get("strategy") is not None:
        raw.setdefault("prd_load_strategy", prd_load.get("strategy"))

    return raw


def _normalize_destination_config(item: Any, connection: dict[str, Any], *, index: int) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise ValueError(f"destinations[{index}] must be a mapping.")

    destination = dict(item)
    destination.setdefault("server", connection.get("server"))
    destination.setdefault("database", connection.get("database"))
    table_defaults = {
        key: destination.get(key)
        for key in ["server", "database", "schema"]
        if destination.get(key) is not None
    }
    for key in ["staging", "staging_table", "destination", "prod", "production", "prod_table", "target_table"]:
        if isinstance(destination.get(key), dict):
            table = dict(destination[key])
            for default_key, default_value in table_defaults.items():
                table.setdefault(default_key, default_value)
            destination[key] = table
    return destination


def _source_column_names(items: list[Any]) -> list[str]:
    names: list[str] = []
    for item in items:
        if isinstance(item, dict):
            source = item.get("source") or item.get("name")
            if source:
                names.append(source)
        else:
            names.append(str(item))
    return names


def _normalize_aux_staging(entry: dict[str, Any], *, index: int) -> dict[str, Any]:
    """Normalize one ``aux_stagings`` YAML entry into the shape AuxStaging.from_dict
    expects: an identity ``name``, a source alias, and a resolved mapping/type bundle
    derived from the entry's own ``field_config`` (same 5-tuple mapping rows and
    type-inference rules as the loader-level field_config). The staging TABLE is NOT
    here -- each destination declares it under ``staging.aux: {name: table}`` -- so
    ``name`` is what binds this entry to a destination's table."""
    source_alias = entry.get("source") or entry.get("source_alias")
    if not source_alias:
        raise ValueError(f"aux_stagings[{index}] requires 'source' (a source-file alias).")
    field_config = dict(entry.get("field_config") or {})
    mapping = [_normalize_mapping_row(item) for item in list(field_config.get("mapping") or [])]
    conversions = (
        field_config.get("type_conversions")
        or field_config.get("type_changes")
        or _type_conversions_from_mapping(mapping)
    )
    return {
        "name": entry.get("name") or source_alias,
        "source_alias": source_alias,
        "column_mapping": mapping,
        "type_changes": {
            "date": list(conversions.get("date", [])),
            "datetime": list(conversions.get("datetime", [])),
            "float": list(conversions.get("float", [])),
            "int": list(conversions.get("int", [])),
        },
        "datetime_to_date_columns": _datetime_to_date_from_mapping(mapping),
        "pk_check": field_config.get("pk_check") or [],
        "date_format": field_config.get("date_format"),
        "datetime_format": field_config.get("datetime_format"),
        "rename_columns": field_config.get("rename_columns") or {},
    }


def _datetime_to_date_from_mapping(mapping: list[dict[str, Any]]) -> list[str]:
    """Source columns typed ``datetime`` in a mapping but whose destination column
    is ``date``: they carry a time-of-day in the file (so a strict date parse would
    coerce them to the fallback) but must land as plain dates. Mirrors
    LoaderConfig.datetime_to_date_columns for an aux mapping."""
    columns: list[str] = []
    for row in mapping:
        if str(row.get("type") or "").strip().lower() != "datetime":
            continue
        if str(row.get("destination_type") or "").strip().lower() != "date":
            continue
        column = row.get("source") or row.get("destination")
        if column:
            columns.append(column)
    return columns


def _normalize_mapping_row(item: Any) -> dict[str, Any]:
    if isinstance(item, dict):
        return dict(item)
    if isinstance(item, (list, tuple)):
        if len(item) != 5:
            raise ValueError(
                "Mapping rows must be "
                f"[source, destination, type, destination_type, origin]: {item}"
            )
        return {
            "source": item[0],
            "destination": item[1],
            "type": item[2],
            "destination_type": item[3],
            "expected_in_source": _origin_expected_in_source(item[4]),
        }
    raise ValueError(f"Unsupported mapping row: {item!r}")


def _origin_expected_in_source(value: Any) -> int:
    """Map a mapping row's ``origin`` keyword to ``expected_in_source``:
    ``source`` -> 1 (read from the source file), ``computed`` -> 0 (produced by a
    transform/loader, so not present in the file).
    """
    origins = {"source": 1, "computed": 0}
    key = str(value).strip().lower()
    if key not in origins:
        raise ValueError(f"mapping row origin must be 'source' or 'computed', got {value!r}.")
    return origins[key]


def _not_loaded_row(name: str, *, required_input: bool) -> dict[str, Any]:
    """Build a column_mapping row for a source column present in the file but not
    loaded to a destination: a transform input (``required_input=True``) or an
    unintegrated 'extra' column (``required_input=False``).

    ``expected_in_source`` is 1 so the presence check runs; ``required_input`` decides
    whether a missing column is a hard failure (True) or a warning (False).
    """
    return {
        "source": name,
        "destination": None,
        "type": "ignore",
        "destination_type": None,
        "expected_in_source": 1,
        "required_input": 1 if required_input else 0,
    }


def _type_conversions_from_mapping(mapping: list[dict[str, Any]]) -> dict[str, list[str]]:
    conversions = {"date": [], "datetime": [], "float": [], "int": []}
    for item in mapping:
        item_type = item.get("type")
        source = item.get("source")
        destination = item.get("destination")
        if item_type in conversions and destination:
            conversions[item_type].append(source or destination)
    return conversions


def select_loaders(
    loaders: list[LoaderConfig],
    *,
    names: list[str] | None = None,
    tags: list[str] | None = None,
) -> list[LoaderConfig]:
    names = names or []
    tags = tags or []
    selected = loaders
    if names:
        wanted = set(names)
        selected = [loader for loader in selected if loader.name in wanted]
    if tags:
        wanted_tags = set(tags)
        selected = [loader for loader in selected if wanted_tags.intersection(loader.tags)]
    return selected


def _daily_loaders(loaders: list[LoaderConfig]) -> list[LoaderConfig]:
    """The loaders selected by ``--all``: every configured loader EXCEPT the
    on-demand ones (tagged :data:`ON_DEMAND_TAG`). On-demand loaders are still
    discovered so they can be run explicitly by name, but they are never swept
    into the daily batch."""
    return [loader for loader in loaders if ON_DEMAND_TAG not in loader.tags]


def get_loader(loaders: list[LoaderConfig], name: str) -> LoaderConfig:
    for loader in loaders:
        if loader.name == name:
            return loader
    raise ValueError(f"No loader named {name!r}")


def run_move_files(
    file_config: str,
    *,
    names: list[str],
    tags: list[str],
    dry_run: bool = False,
    list_only: bool = False,
    check: bool = False,
    loader_config_ref: str = "configs",
) -> int:
    """Run the file-folder dispatcher: relocate each downloaded export to its
    input's designated folder + canonical name (the central registry the
    loaders also resolve from). Filesystem only -- no database is touched, so
    unlike ``run`` there is no confirmation prompt and ``--dry-run`` previews
    everything."""
    try:
        registry = load_input_registry(file_config)
        selected = select_inputs(registry, names=names, tags=tags)
    except (OSError, ValueError) as exc:
        print(f"move-files: {exc}", file=sys.stderr)
        return 2

    if list_only:
        for item in selected:
            download = item.download
            patterns = " | ".join(str(Path(download.source_dir) / pattern) for pattern in download.patterns)
            print(f"{item.key}\t{','.join(item.tags)}\t{patterns}  ->  {item.full_path}")
        return 0

    if not selected:
        print("No downloadable inputs selected.", file=sys.stderr)
        return 2

    results = run_moves(selected, dry_run=dry_run, logger=_move_files_logger())
    for result in results:
        print(f"{result.status}\t{result.key}\t{result.detail}")
    ok = not any(result.failed for result in results)

    if check:
        ok = _check_loader_sources(loader_config_ref) and ok
    return 0 if ok else 1


def _move_files_logger() -> logging.Logger:
    logger = logging.getLogger("infor_loader.move_files")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def _check_loader_sources(loader_config_ref: str) -> bool:
    """Verify every enabled loader can resolve each of its source files after a
    dispatch. Prints one FOUND/MISSING line per source file and returns False if
    any is missing. On-demand loaders (tagged ``ON_DEMAND_TAG``) are skipped:
    their input is not expected to be present between the manual runs, so a
    missing on-demand file must not fail the daily dispatch check."""
    loaders = [
        loader
        for loader in load_loader_configs(loader_config_ref)
        if loader.enabled and ON_DEMAND_TAG not in loader.tags
    ]
    all_found = True
    for loader in loaders:
        for source_file in loader.source_files:
            expected = str(Path(source_file.path) / source_file.name)
            try:
                resolved = source_file.resolve()
                detail = expected if str(resolved) == expected else f"{expected}  ->  {resolved}"
                print(f"FOUND\t{loader.name}\t{detail}")
            except FileNotFoundError:
                all_found = False
                print(f"MISSING\t{loader.name}\t{expected}")
    return all_found


PHASE_LABELS = {
    "both": "staging load + prod promotion",
    "stg": "staging load ONLY (prod promotion skipped)",
    "prd": "prod promotion ONLY (source file/staging load skipped)",
}


def _add_phase_flags(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--stg-only",
        action="store_true",
        help="Load the staging table from the file only; skip the staging->prod promotion.",
    )
    group.add_argument(
        "--prd-only",
        action="store_true",
        help="Run the staging->prod promotion only; skip reading the file and loading staging.",
    )


def _resolve_max_workers(args: argparse.Namespace) -> int:
    requested = args.max_workers
    if requested is None:
        return MAX_WORKERS_CAP if getattr(args, "all", False) else 1
    if requested > MAX_WORKERS_CAP:
        print(f"--max-workers {requested} exceeds the cap; using {MAX_WORKERS_CAP}.", file=sys.stderr)
        return MAX_WORKERS_CAP
    return max(1, requested)


def _resolve_phase(args: argparse.Namespace) -> str:
    if getattr(args, "prd_only", False):
        return "prd"
    if getattr(args, "stg_only", False):
        return "stg"
    return "both"


def run_interactive(
    config_ref: str,
    *,
    names: list[str],
    tags: list[str],
    auto: bool,
    max_workers: int,
    log_root: str | None,
    phase: str = "both",
    select_all: bool = False,
    ignore_download: bool = False,
) -> int:
    """Print a summary of the selected loaders and run them after confirmation."""
    loaders = load_loader_configs(config_ref)
    selected = _daily_loaders(loaders) if select_all else select_loaders(loaders, names=names, tags=tags)
    if not selected:
        wanted = "--all" if select_all else (", ".join([*names, *tags]) or "(none)")
        print(f"No loaders matched: {wanted}", file=sys.stderr)
        return 2

    print(f"Mode: {PHASE_LABELS.get(phase, phase)}")
    if phase != "prd":
        if ignore_download:
            print("Download check: OFF (--ignore-download; loading the file already in place)")
        else:
            print("Download check: ON (skip + log FILE_NOT_FOUND for any loader with no fresh export in Downloads)")
    print()
    for loader in selected:
        print_loader_summary(loader, phase=phase)
        print()

    enabled = [loader for loader in selected if loader.enabled]
    if not enabled:
        print("No enabled loaders selected; nothing to run.", file=sys.stderr)
        return 2

    if not auto and not confirm_proceed(enabled, phase=phase):
        print("Aborted; nothing was run.")
        return 1

    results = run_loaders(
        enabled,
        max_workers=max_workers,
        log_root=log_root,
        phase=phase,
        registry=_find_registry_for(config_ref),
        ignore_download=ignore_download,
    )
    for result in results:
        row_text = "" if result.row_count is None else f" rows={result.row_count}"
        print(f"{result.status}\t{result.loader_name}\t{result.duration_seconds}s{row_text}\t{result.log_file_path}")
    # A FILE_NOT_FOUND skip (no fresh download) is a benign, expected outcome,
    # not a failure: it must not turn the whole run's exit code non-zero.
    return 0 if all(result.status in {"SUCCESS", "FILE_NOT_FOUND"} for result in results) else 1


def print_loader_summary(loader: LoaderConfig, *, phase: str = "both") -> None:
    print(f"Loader: {loader.name}")
    print(f"  enabled: {loader.enabled}")
    print(f"  tags:    {', '.join(loader.tags) if loader.tags else '(none)'}")
    if phase == "prd":
        print("  source files: (skipped - --prd-only reads no file)")
    else:
        print("  source files:")
        for source_file in loader.source_files:
            location = str(Path(source_file.path) / source_file.name)
            try:
                resolved = source_file.resolve()
                detail = location if str(resolved) == location else f"{location}  ->  {resolved}"
            except FileNotFoundError:
                detail = f"{location}  (NOT FOUND)"
            print(f"    - [{source_file.alias}] {detail}")
    print("  destinations:")
    for destination in loader.destinations:
        print(f"    - {destination.display_name(include_server=True)}")
    enabled_destinations = [destination for destination in loader.destinations if destination.enabled]
    if enabled_destinations and all(destination.is_direct for destination in enabled_destinations):
        print(f"  load:    direct={loader.stg_load_strategy} (no staging table; the final table is loaded in place)")
    else:
        prod_strategy = loader.prd_load_strategy or "(none)"
        print(f"  load:    staging={loader.stg_load_strategy}, prod={prod_strategy}")
    print(f"  logs:    {loader.log_root}")
    print(f"  archive: {loader.archive_dir or '(disabled)'}")


def confirm_proceed(loaders: list[LoaderConfig], *, phase: str = "both") -> bool:
    names = ", ".join(loader.name for loader in loaders)
    print(f"About to run {len(loaders)} loader(s): {names}")
    print(f"Mode: {PHASE_LABELS.get(phase, phase)}")
    if phase == "stg":
        print("This writes to the STAGING tables only; prod is left untouched.")
    elif phase == "prd":
        print("This runs prod promotion using whatever is ALREADY in the staging tables.")
    else:
        print("This writes to the staging/prod tables shown above.")
    while True:
        try:
            answer = input("Proceed? Type 'yes' to run, 'no' to exit: ").strip().lower()
        except EOFError:
            return False
        if answer in {"yes", "y"}:
            return True
        if answer in {"no", "n"}:
            return False
        print("Please type 'yes' or 'no'.")


def run_loaders(
    loaders: list[LoaderConfig],
    *,
    max_workers: int,
    log_root: str,
    phase: str = "both",
    registry: InputRegistry | None = None,
    ignore_download: bool = False,
) -> list[LoaderResult]:
    # Each loader's downloadable inputs, resolved once from the registry. Unless
    # --ignore-download is set, the loader gates itself on a fresh download for
    # every one of these before touching the DB (see FileLoader.run).
    download_inputs = {loader.name: _resolve_download_inputs(loader, registry) for loader in loaders}

    if max_workers <= 1:
        return [
            FileLoader(
                loader,
                log_root=log_root,
                phase=phase,
                download_inputs=download_inputs[loader.name],
                ignore_download=ignore_download,
            ).run()
            for loader in loaders
        ]

    # stdout/stderr capture redirects process-global streams, so it is unsafe
    # across threads; let each loader's framework file logging handle parallel runs.
    results: list[LoaderResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_loader = {
            executor.submit(
                FileLoader(
                    loader,
                    log_root=log_root,
                    capture_streams=False,
                    phase=phase,
                    download_inputs=download_inputs[loader.name],
                    ignore_download=ignore_download,
                ).run
            ): loader
            for loader in loaders
        }
        for future in as_completed(future_to_loader):
            results.append(future.result())
    results.sort(key=lambda result: [loader.name for loader in loaders].index(result.loader_name))
    return results


def _load_config_module(config_ref: str):
    config_path = Path(config_ref)
    if config_path.suffix == ".py" or config_path.exists():
        module_name = f"_infor_loader_config_{config_path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, config_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load config from {config_ref}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    return importlib.import_module(config_ref)


if __name__ == "__main__":
    raise SystemExit(main())
