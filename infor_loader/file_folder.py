"""Central registry for every loader's input files (the SOURCE side).

``configs/file_folder_loader_config.yaml`` is the single source of truth for
*where each input file lives* and *how it arrives*. It has two blocks:

``folders``
    Named locations (``downloads``, ``temp_export``, ``misc_mdm``). This is the
    batch-change knob: relocate a folder here and every loader + download rule
    that references it by name follows. Absolute paths live ONLY here.

``inputs``
    One entry per physical input file: which ``folder`` it lives in, its
    canonical ``name``, and -- for files that arrive as manual downloads -- a
    ``download`` block (the glob ``patterns`` to match in the download folder).
    A fixture that is maintained in place (e.g. ``company_map.csv``) simply has
    no ``download`` block.

Two consumers resolve from this one registry, so they cannot drift:
  * the loaders -- each ``source.files`` entry references an input by key
    (``input: poline``) instead of restating its ``path``/``name`` (see
    ``infor_loader/cli.py`` expansion); and
  * ``run_daily_loaders.py move-files`` -- relocates each downloaded export to
    its input's folder + canonical name before the loaders run.

This module manages the filesystem only: it never reads file contents and never
touches a database. It is distinct from a loader's own
``pre_file_moves``/``post_file_moves`` hooks (which run inside that loader's
run) -- this is the standalone dispatcher for the whole day's downloads.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# The registry lives at the configs\ root (loaders sit in configs\loaders\) and
# is NOT a loader config; load_loader_configs skips these names when globbing.
CONFIG_FILENAMES = frozenset({"file_folder_loader_config.yaml", "file_folder_loader_config.yml"})
DEFAULT_CONFIG_PATH = str(Path("configs") / "file_folder_loader_config.yaml")

MOVE_ACTIONS = frozenset({"move", "copy"})

# What to do when the destination file already exists:
#   replace - overwrite it (the loaders should read the freshest download; the
#             per-loader archive step keeps the history)
#   skip    - leave the existing file, do not consume the download
#   fail    - treat it as an error for that input
ON_EXISTS = frozenset({"replace", "skip", "fail"})

# Folder every download is matched in unless an input's download block overrides
# it. A registry key, resolved against `folders`.
DEFAULT_DOWNLOAD_FROM = "downloads"

# Inputs (and loaders) carrying this tag are ON-DEMAND: excluded from the
# unfiltered daily selection -- `move-files` with no --input/--tag here, and the
# loader `--all` batch in cli.py -- but still selectable explicitly by key or tag.
# For an input this keeps a manual export from being relocated out of Downloads
# before its on-demand loader runs (the loader's own download gate stages it).
ON_DEMAND_TAG = "on-demand"


@dataclass(frozen=True)
class DownloadSpec:
    """How a downloaded export is matched and relocated into its input folder."""

    source_dir: str                      # resolved absolute path of the `from` folder
    patterns: tuple[str, ...]
    action: str = "move"
    # Newest match (by mtime) wins; the older duplicates are left in place.
    pick_latest: bool = True
    # required=False: an input with nothing in the download folder only warns --
    # the real presence guard is `move-files --check` / the loader's own
    # FILE NOT FOUND. On any given day only some exports are re-downloaded, and
    # the file already sitting in the designated folder is still valid.
    required: bool = False
    on_exists: str = "replace"


@dataclass(frozen=True)
class InputFile:
    """One physical input file: where it lives + (optionally) how it arrives."""

    key: str
    folder: str                          # `folders` registry key
    destination_dir: str                 # resolved absolute path of `folder`
    name: str                            # canonical file name the loaders read
    download: DownloadSpec | None = None
    tags: tuple[str, ...] = ()

    @property
    def path(self) -> str:
        """The folder a loader reads this file from (its designated location)."""
        return self.destination_dir

    @property
    def full_path(self) -> str:
        return str(Path(self.destination_dir) / self.name)


@dataclass(frozen=True)
class InputRegistry:
    folders: dict[str, str] = field(default_factory=dict)
    inputs: dict[str, InputFile] = field(default_factory=dict)

    def resolve(self, key: str) -> InputFile:
        try:
            return self.inputs[key]
        except KeyError:
            known = ", ".join(sorted(self.inputs)) or "(none)"
            raise ValueError(f"Unknown input {key!r}. Known inputs: {known}") from None

    def downloadable(self) -> list[InputFile]:
        """Inputs that arrive as downloads (have a download block), in file order."""
        return [item for item in self.inputs.values() if item.download is not None]


def load_input_registry(path: str | Path) -> InputRegistry:
    """Parse the file-folder registry into folders + inputs.

    ``download_defaults`` supplies the fallback ``from`` folder and move
    behavior every input's ``download`` block inherits. Unknown folder
    references (an input's ``folder`` or a download's ``from``) are a hard,
    early error so a typo never silently sends a file to the wrong place.
    """
    import yaml

    config_path = Path(path)
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"File-folder registry must be a mapping: {config_path}")

    folders = {str(name): str(value) for name, value in (data.get("folders") or {}).items()}
    if not folders:
        raise ValueError(f"File-folder registry must define a non-empty 'folders' map: {config_path}")

    defaults = dict(data.get("download_defaults") or {})
    default_from = str(defaults.get("from", DEFAULT_DOWNLOAD_FROM))

    raw_inputs = data.get("inputs") or {}
    if not isinstance(raw_inputs, dict):
        raise ValueError(f"File-folder registry 'inputs' must be a mapping: {config_path}")

    inputs: dict[str, InputFile] = {}
    for key, spec in raw_inputs.items():
        key = str(key)
        if not isinstance(spec, dict):
            raise ValueError(f"input {key!r} must be a mapping.")
        folder = spec.get("folder")
        if folder not in folders:
            raise ValueError(f"input {key!r}: folder {folder!r} is not defined in 'folders'.")
        name = spec.get("name")
        if not name:
            raise ValueError(f"input {key!r} must define 'name'.")

        download = _download_spec_from(spec.get("download"), key, folders, defaults, default_from)
        inputs[key] = InputFile(
            key=key,
            folder=str(folder),
            destination_dir=folders[folder],
            name=str(name),
            download=download,
            tags=tuple(str(tag) for tag in (spec.get("tags") or [])),
        )
    return InputRegistry(folders=folders, inputs=inputs)


def _download_spec_from(
    raw: Any,
    key: str,
    folders: dict[str, str],
    defaults: dict[str, Any],
    default_from: str,
) -> DownloadSpec | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError(f"input {key!r}: download must be a mapping.")

    from_folder = str(raw.get("from", default_from))
    if from_folder not in folders:
        raise ValueError(f"input {key!r}: download.from {from_folder!r} is not defined in 'folders'.")

    raw_patterns = raw.get("patterns")
    if raw_patterns is None and raw.get("pattern") is not None:
        raw_patterns = [raw["pattern"]]
    patterns = tuple(str(pattern) for pattern in (raw_patterns or []) if str(pattern).strip())
    if not patterns:
        raise ValueError(f"input {key!r}: download must define 'patterns' (or 'pattern').")

    action = str(raw.get("action", defaults.get("action", "move"))).strip().lower()
    if action not in MOVE_ACTIONS:
        allowed = ", ".join(sorted(MOVE_ACTIONS))
        raise ValueError(f"input {key!r}: download.action must be one of: {allowed}; got {action!r}.")
    on_exists = str(raw.get("on_exists", defaults.get("on_exists", "replace"))).strip().lower()
    if on_exists not in ON_EXISTS:
        allowed = ", ".join(sorted(ON_EXISTS))
        raise ValueError(f"input {key!r}: download.on_exists must be one of: {allowed}; got {on_exists!r}.")

    return DownloadSpec(
        source_dir=folders[from_folder],
        patterns=patterns,
        action=action,
        pick_latest=bool(raw.get("pick_latest", defaults.get("pick_latest", True))),
        required=bool(raw.get("required", defaults.get("required", False))),
        on_exists=on_exists,
    )


def select_inputs(
    registry: InputRegistry,
    *,
    names: list[str] | None = None,
    tags: list[str] | None = None,
) -> list[InputFile]:
    """Downloadable inputs matching the filters, in registry (file) order.

    A ``names`` entry that is a known input but not downloadable (a fixture) is
    an error -- selecting a fixture to "move" is a mistake worth surfacing.

    Unfiltered (no ``names`` and no ``tags``) is the daily dispatch: on-demand
    inputs (tagged :data:`ON_DEMAND_TAG`) are skipped so a manual export is not
    pulled out of Downloads before its on-demand loader runs. An explicit
    ``--input <key>`` or ``--tag on-demand`` still selects them."""
    names = names or []
    tags = tags or []
    selected = registry.downloadable()
    if not names and not tags:
        return [item for item in selected if ON_DEMAND_TAG not in item.tags]
    if names:
        wanted = set(names)
        unknown = wanted - set(registry.inputs)
        if unknown:
            raise ValueError(f"No input named: {', '.join(sorted(unknown))}")
        fixtures = wanted - {item.key for item in selected}
        if fixtures:
            raise ValueError(
                f"Input(s) {', '.join(sorted(fixtures))} are fixtures (no download block); nothing to move."
            )
        selected = [item for item in selected if item.key in wanted]
    if tags:
        wanted_tags = set(tags)
        selected = [item for item in selected if wanted_tags.intersection(item.tags)]
    return selected


@dataclass(frozen=True)
class MoveResult:
    key: str
    # MOVED | COPIED | DRY_RUN | NO_MATCH | SKIPPED | ERROR
    status: str
    detail: str
    files: tuple[tuple[str, str], ...] = ()

    @property
    def failed(self) -> bool:
        return self.status == "ERROR"


def run_moves(
    inputs: list[InputFile],
    *,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> list[MoveResult]:
    """Relocate each input's download independently: one input's failure never
    blocks the rest of the day's files (mirrors how one loader's failure does
    not stop the other loaders). The caller decides the exit code."""
    logger = logger or logging.getLogger(__name__)
    return [_run_move(item, dry_run=dry_run, logger=logger) for item in inputs]


def _run_move(item: InputFile, *, dry_run: bool, logger: logging.Logger) -> MoveResult:
    download = item.download
    if download is None:  # defensive: run_moves is only handed downloadable inputs
        return MoveResult(item.key, "SKIPPED", "no download block (fixture)")
    try:
        source_dir = Path(download.source_dir)
        matches: list[Path] = []
        if source_dir.is_dir():
            found: set[Path] = set()
            for pattern in download.patterns:
                found.update(path for path in source_dir.glob(pattern) if path.is_file())
            matches = sorted(found, key=lambda path: path.stat().st_mtime)

        if not matches:
            watched = ", ".join(str(source_dir / pattern) for pattern in download.patterns)
            if download.required:
                logger.error("[%s] no file matched %s", item.key, watched)
                return MoveResult(item.key, "ERROR", f"required, but no file matched {watched}")
            logger.warning("[%s] no file matched %s; skipping.", item.key, watched)
            return MoveResult(item.key, "NO_MATCH", f"no file matched {watched}")

        files = [matches[-1]] if download.pick_latest else matches
        destination_dir = Path(item.destination_dir)
        performed: list[tuple[str, str]] = []
        skipped: list[str] = []
        for file_path in files:
            # Every match is normalized to the input's canonical name.
            target = destination_dir / item.name
            if target.exists() and file_path.samefile(target):
                skipped.append(f"{file_path} already IS {target}")
                continue
            if target.exists():
                if download.on_exists == "fail":
                    return MoveResult(item.key, "ERROR", f"destination exists: {target}")
                if download.on_exists == "skip":
                    logger.info("[%s] destination exists, leaving it: %s", item.key, target)
                    skipped.append(f"kept existing {target}")
                    continue
                if not dry_run:
                    target.unlink()
            if dry_run:
                logger.info("[%s] DRY RUN would %s %s -> %s", item.key, download.action, file_path, target)
            else:
                destination_dir.mkdir(parents=True, exist_ok=True)
                if download.action == "copy":
                    shutil.copy2(file_path, target)
                else:
                    shutil.move(str(file_path), str(target))
                logger.info("[%s] %s %s -> %s", item.key, download.action, file_path, target)
            performed.append((str(file_path), str(target)))

        if not performed:
            return MoveResult(item.key, "SKIPPED", "; ".join(skipped) or "nothing to do")
        status = "DRY_RUN" if dry_run else ("COPIED" if download.action == "copy" else "MOVED")
        detail = "; ".join(f"{source} -> {target}" for source, target in performed)
        return MoveResult(item.key, status, detail, tuple(performed))
    except OSError as exc:
        logger.exception("[%s] file move failed", item.key)
        return MoveResult(item.key, "ERROR", str(exc))
