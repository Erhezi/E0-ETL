from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .config import LoaderConfig, TableRef
from .file_loader import FileLoader, LoaderResult
from .utilities import build_column_mapping_template, inspect_table, write_mapping_template


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run configurable Infor file loaders.")
    parser.add_argument("--config", default="configs", help="Config directory, .yaml file, Python module, or .py path.")

    # Top-level run shortcut: `run_daily_loaders.py --loader <name>` prints a
    # summary and asks for confirmation before running; `--auto` skips the prompt.
    parser.add_argument("--loader", action="append", default=[], help="Loader name to run. Can be repeated.")
    parser.add_argument("--tag", action="append", default=[], help="Select loaders by tag. Can be repeated.")
    parser.add_argument("--auto", action="store_true", help="Skip the confirmation prompt and run immediately.")
    parser.add_argument("--max-workers", type=int, default=1, help="Parallel worker count. Use 1 for sequential.")
    parser.add_argument("--log-root", default=None, help="Override each loader's YAML logging.log_root.")
    _add_phase_flags(parser)

    subparsers = parser.add_subparsers(dest="command", required=False)

    list_parser = subparsers.add_parser("list", help="List configured loaders.")
    list_parser.add_argument("--tag", action="append", default=[])

    run_parser = subparsers.add_parser("run", help="Run configured loaders.")
    run_parser.add_argument("--loader", action="append", default=[], help="Loader name to run. Can be repeated.")
    run_parser.add_argument("--tag", action="append", default=[], help="Run loaders with tag. Can be repeated.")
    run_parser.add_argument("--max-workers", type=int, default=1, help="Parallel worker count. Use 1 for sequential.")
    run_parser.add_argument("--log-root", default=None, help="Override each loader's YAML logging.log_root.")
    _add_phase_flags(run_parser)

    mapping_parser = subparsers.add_parser("mapping-template", help="Generate a destination-column mapping template.")
    mapping_parser.add_argument("--loader", required=True)
    mapping_parser.add_argument("--output", required=True)

    table_parser = subparsers.add_parser("table-info", help="Print SQL Server destination table columns.")
    table_parser.add_argument("--server", required=True)
    table_parser.add_argument("--database", required=True)
    table_parser.add_argument("--schema", required=True)
    table_parser.add_argument("--table", required=True)

    args = parser.parse_args(argv)

    if args.command is None:
        if args.loader or args.tag:
            return run_interactive(
                args.config,
                names=args.loader,
                tags=args.tag,
                auto=args.auto,
                max_workers=args.max_workers,
                log_root=args.log_root,
                phase=_resolve_phase(args),
            )
        parser.print_help(sys.stderr)
        return 2

    if args.command == "table-info":
        table = TableRef(args.server, args.database, args.schema, args.table)
        print(json.dumps(inspect_table(table), indent=2, default=str))
        return 0

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
        selected = select_loaders(loaders, names=args.loader, tags=args.tag)
        selected = [loader for loader in selected if loader.enabled]
        if not selected:
            print("No enabled loaders selected.", file=sys.stderr)
            return 2
        results = run_loaders(
            selected,
            max_workers=args.max_workers,
            log_root=args.log_root,
            phase=_resolve_phase(args),
        )
        for result in results:
            row_text = "" if result.row_count is None else f" rows={result.row_count}"
            print(f"{result.status}\t{result.loader_name}\t{result.duration_seconds}s{row_text}\t{result.log_file_path}")
        return 0 if all(result.status == "SUCCESS" for result in results) else 1

    raise ValueError(f"Unknown command: {args.command}")


def load_loader_configs(config_ref: str) -> list[LoaderConfig]:
    config_path = Path(config_ref)
    if config_path.exists():
        if config_path.is_dir():
            yaml_paths = sorted([*config_path.glob("*.yaml"), *config_path.glob("*.yml")])
            return [_load_yaml_config(path) for path in yaml_paths]
        if config_path.suffix.lower() in {".yaml", ".yml"}:
            return [_load_yaml_config(config_path)]

    module = _load_config_module(config_ref)
    health_table = TableRef.from_dict(module.HEALTH_TABLE) if hasattr(module, "HEALTH_TABLE") else None
    raw_loaders: list[dict[str, Any]]
    if hasattr(module, "get_loaders"):
        raw_loaders = module.get_loaders()
    else:
        raw_loaders = module.LOADERS
    return [LoaderConfig.from_dict(raw, health_table=health_table) for raw in raw_loaders]


def _load_yaml_config(path: Path) -> LoaderConfig:
    import yaml

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"YAML config must contain a mapping: {path}")
    raw = _normalize_yaml_config(data)
    return LoaderConfig.from_dict(raw)


def _normalize_yaml_config(data: dict[str, Any]) -> dict[str, Any]:
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

    process = raw.get("process")
    if isinstance(process, dict):
        if process.get("name") is not None:
            raw.setdefault("process_name", process.get("name"))
        if process.get("frequency") is not None:
            raw.setdefault("process_frequency", process.get("frequency"))

    stg_load = raw.get("stg_load")
    if stg_load is None and isinstance(raw.get("load"), dict):
        stg_load = raw.get("load")  # back-compat: staging load block used to be `load`
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


def get_loader(loaders: list[LoaderConfig], name: str) -> LoaderConfig:
    for loader in loaders:
        if loader.name == name:
            return loader
    raise ValueError(f"No loader named {name!r}")


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
) -> int:
    """Print a summary of the selected loaders and run them after confirmation."""
    loaders = load_loader_configs(config_ref)
    selected = select_loaders(loaders, names=names, tags=tags)
    if not selected:
        wanted = ", ".join([*names, *tags]) or "(none)"
        print(f"No loaders matched: {wanted}", file=sys.stderr)
        return 2

    print(f"Mode: {PHASE_LABELS.get(phase, phase)}")
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

    results = run_loaders(enabled, max_workers=max_workers, log_root=log_root, phase=phase)
    for result in results:
        row_text = "" if result.row_count is None else f" rows={result.row_count}"
        print(f"{result.status}\t{result.loader_name}\t{result.duration_seconds}s{row_text}\t{result.log_file_path}")
    return 0 if all(result.status == "SUCCESS" for result in results) else 1


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
) -> list[LoaderResult]:
    if max_workers <= 1:
        return [FileLoader(loader, log_root=log_root, phase=phase).run() for loader in loaders]

    # stdout/stderr capture redirects process-global streams, so it is unsafe
    # across threads; let each loader's framework file logging handle parallel runs.
    results: list[LoaderResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_loader = {
            executor.submit(FileLoader(loader, log_root=log_root, capture_streams=False, phase=phase).run): loader
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
