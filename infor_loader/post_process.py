"""Post-load processes: the SQL / Python jobs that run once the daily loaders land.

Three of them run after the daily batch -- PLM, Preprocessor and BullardBurnDown --
none depending on each other, each depending on specific loaders having landed
cleanly. Each is declared in ``configs/post_processes/<name>.yaml`` as an ordered
list of steps against one or more destinations, and each writes exactly ONE
ETLHealth row per destination carrying the top-level status. Per-step detail (which
sub-procedure failed, how long each took) stays in the run's log file, which the
daily report attaches on failure -- that is the whole point of the rollup: the two
batch procs already log every sub-procedure to their own process_log tables, so
ETLHealth only needs the verdict.

Before a process runs, :func:`check_requirements` reads today's ETLHealth for the
loaders it requires. A requirement that is not SUCCESS blocks that destination: it
is recorded BLOCKED, no statement is executed, and the run's exit code is
unaffected -- the upstream loader's own FAILED row is the real alarm, and
double-reporting it would just make the nightly job look broken twice.

Entry point: :func:`run_post_processes`, wired into the CLI as the ``post-run``
subcommand and the ``--post-run`` flag on a batch run.
"""

from __future__ import annotations

import datetime as dt
import importlib
import logging
import sys
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

from .config import TableRef, apply_loader_name, bracket_identifier
from .db import connect_sql_server, create_sqlalchemy_engine, insert_health_record
from .file_loader import _capture_streams, build_run_logger, classify_error, close_logger


# Python steps are resolved relative to this package unless the reference names a
# module path of its own (``pkg.module:callable``).
PROCESS_PACKAGE = "infor_loader.processes"

# Directory (under the configs root) the post-process YAMLs live in. Deliberately a
# sibling of `loaders/`, which is what cli.load_loader_configs recurses -- so these
# are never swept into the `--all` loader batch.
POST_PROCESS_DIRNAME = "post_processes"

STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
#: Requirements unmet -- the process did not run and touched no table. Benign for
#: the exit code, exactly like a loader's FILE_NOT_FOUND skip.
STATUS_BLOCKED = "BLOCKED"
#: A step that never started because an earlier step in the same destination failed.
STATUS_NOT_RUN = "NOT RUN"

#: Destinations run concurrently by default: des1 and des2 are different servers, so
#: there is nothing for them to contend over and a process finishes in max(des1, des2)
#: rather than the sum. Naturally bounded by how many destinations are enabled -- with
#: des2 not yet deployed this changes nothing.
DEFAULT_DESTINATION_WORKERS = 2
#: Sanity bound; there are only ever a handful of destinations.
DESTINATION_WORKERS_CAP = 4

#: ETLHealth TargetTableType for a post-process row. These rows describe a procedure
#: run, not a staging or production table load, so they take their own type rather
#: than borrowing STG/PRD. (Column is varchar(10).)
TARGET_TABLE_TYPE = "PROC"

# ETLHealth column widths that the rolled-up values can realistically reach.
_MAX_TARGET_TABLE_NAME = 100
_MAX_ERROR = 400

# What an unmet requirement does.
UNMET_MODES = frozenset({"block", "warn"})
# Which ETLHealth rows a requirement is checked against.
REQUIREMENT_SCOPES = frozenset({"destination", "any"})


# ── Config ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class ProcessStep:
    """One unit of work inside a post-process: a T-SQL statement (``exec``) or a
    Python callable (``python``), never both.

    ``target`` names the table or procedure this step drives, for the ETLHealth
    ``TargetTableName`` column. When omitted it is derived from the EXEC'd object
    name, which is right for the batch procs and keeps the config short.
    """

    name: str
    exec_sql: str | None = None
    python: str | None = None
    params: tuple[Any, ...] = ()
    target: str | None = None
    options: dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, index: int, process: str) -> "ProcessStep":
        if not isinstance(data, dict):
            raise ValueError(f"{process}: steps[{index}] must be a mapping.")
        exec_sql = data.get("exec") or data.get("sql")
        python = data.get("python")
        if bool(exec_sql) == bool(python):
            raise ValueError(
                f"{process}: steps[{index}] must set exactly one of 'exec' or 'python'."
            )
        name = data.get("name")
        if not name:
            raise ValueError(f"{process}: steps[{index}] requires a 'name'.")
        timeout = data.get("timeout_seconds")
        return cls(
            name=str(name),
            exec_sql=str(exec_sql) if exec_sql else None,
            python=str(python) if python else None,
            params=tuple(data.get("params") or ()),
            target=data.get("target"),
            options=dict(data.get("options") or {}),
            timeout_seconds=int(timeout) if timeout is not None else None,
        )

    @property
    def kind(self) -> str:
        """ETLHealth ProcessType contribution: what this step actually runs."""
        return "stored_procedure" if self.exec_sql else "python"

    def target_label(self, destination: "ProcessDestination") -> str:
        """``[db].[schema].[object]`` for this step's ETLHealth TargetTableName."""
        raw = self.target or self._derived_target()
        if not raw:
            return f"{self.name}"
        return _qualify_object(raw, destination)

    def _derived_target(self) -> str | None:
        """The object an ``exec`` step drives, read off the statement itself; the
        callable reference for a ``python`` step with no declared target."""
        if self.exec_sql:
            return _object_name_from_exec(self.exec_sql)
        return self.python


@dataclass(frozen=True)
class ProcessDestination:
    """Where a post-process runs, named ``des1``/``des2`` like the loaders so
    ``--destination`` addresses the same side across both."""

    name: str
    server: str
    database: str
    schema: str | None
    steps: tuple[ProcessStep, ...]
    enabled: bool = True

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, index: int, process: str) -> "ProcessDestination":
        if not isinstance(data, dict):
            raise ValueError(f"{process}: destinations[{index}] must be a mapping.")
        name = str(data.get("name") or data.get("alias") or f"destination_{index + 1}")
        for key in ("server", "database"):
            if not data.get(key):
                raise ValueError(f"{process}: destination {name!r} requires '{key}'.")
        raw_steps = data.get("steps") or []
        if not raw_steps:
            raise ValueError(f"{process}: destination {name!r} declares no steps.")
        return cls(
            name=name,
            server=str(data["server"]),
            database=str(data["database"]),
            schema=str(data["schema"]) if data.get("schema") else None,
            steps=tuple(
                ProcessStep.from_dict(dict(step), index=step_index, process=f"{process}/{name}")
                for step_index, step in enumerate(raw_steps)
            ),
            enabled=bool(data.get("enabled", True)),
        )

    def display_name(self) -> str:
        location = f"{self.server}.{self.database}"
        if self.schema:
            location = f"{location}.{self.schema}"
        return f"{self.name}: {location} ({len(self.steps)} step(s))"


@dataclass(frozen=True)
class Requirement:
    """Loaders that must show SUCCESS in today's ETLHealth before a process runs.

    ``loaders`` holds loader *names* (``inventory_location``), not the friendly
    ETLHealth ProcessName -- the runner resolves one to the other through the loader
    configs, so the display names live in exactly one place and cannot drift.

    ``scope`` decides which rows count. ``destination`` (the default) checks only the
    rows written on the server this process is about to run against, so a des2
    promotion failure never blocks des1 work that is perfectly fine. ``any`` requires
    the loader to be clean everywhere it ran.
    """

    loaders: tuple[str, ...] = ()
    scope: str = "destination"
    on_unmet: str = "block"

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None, *, process: str) -> "Requirement":
        if not data:
            return cls()
        loaders = data.get("loaders") or data.get("loader") or []
        if isinstance(loaders, str):
            loaders = [loaders]
        scope = str(data.get("scope", "destination")).strip().lower()
        if scope not in REQUIREMENT_SCOPES:
            allowed = ", ".join(sorted(REQUIREMENT_SCOPES))
            raise ValueError(f"{process}: requires.scope must be one of: {allowed}; got {scope!r}.")
        on_unmet = str(data.get("on_unmet", "block")).strip().lower()
        if on_unmet not in UNMET_MODES:
            allowed = ", ".join(sorted(UNMET_MODES))
            raise ValueError(f"{process}: requires.on_unmet must be one of: {allowed}; got {on_unmet!r}.")
        return cls(loaders=tuple(str(name) for name in loaders), scope=scope, on_unmet=on_unmet)


@dataclass
class PostProcessConfig:
    name: str
    process_name: str
    destinations: list[ProcessDestination]
    requires: Requirement = field(default_factory=Requirement)
    health_table: TableRef | None = None
    process_frequency: str = "Daily"
    log_root: str = "logs"
    log_level: str = "INFO"
    log_to_console: bool = True
    capture_streams: bool = True
    enabled: bool = True
    tags: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PostProcessConfig":
        name = data.get("name")
        if not name:
            raise ValueError("post-process config requires a 'name'.")
        name = str(name)
        process = dict(data.get("process") or {})
        logging_config = dict(data.get("logging") or {})
        raw_destinations = data.get("destinations")
        if not raw_destinations:
            raise ValueError(f"{name}: post-process config must define destinations.")
        destinations = [
            ProcessDestination.from_dict(dict(item), index=index, process=name)
            for index, item in enumerate(raw_destinations)
        ]
        if not any(destination.enabled for destination in destinations):
            raise ValueError(
                f"{name}: every destination is disabled; enable at least one or "
                "disable the process itself."
            )
        return cls(
            name=name,
            process_name=str(process.get("name") or data.get("process_name") or name),
            destinations=destinations,
            requires=Requirement.from_dict(data.get("requires"), process=name),
            health_table=TableRef.from_dict(data["health_table"]) if data.get("health_table") else None,
            process_frequency=str(process.get("frequency") or data.get("process_frequency") or "Daily"),
            log_root=apply_loader_name(logging_config.get("log_root") or data.get("log_root") or "logs", name),
            log_level=str(logging_config.get("level", data.get("log_level", "INFO"))),
            log_to_console=bool(logging_config.get("console", data.get("log_to_console", True))),
            capture_streams=bool(logging_config.get("capture_streams", data.get("capture_streams", True))),
            enabled=bool(data.get("enabled", True)),
            tags=[str(tag) for tag in (data.get("tags") or [])],
        )


def load_post_process_configs(config_ref: str = "configs") -> list[PostProcessConfig]:
    """Load every post-process YAML for a config reference.

    A directory resolves to its ``post_processes/`` subfolder (the loaders live in
    the sibling ``loaders/``); a single ``.yaml`` path loads just that file, so one
    process can be validated on its own. A missing folder is not an error -- it just
    means nothing is configured to run after the loaders.
    """
    import yaml

    config_path = Path(config_ref)
    if config_path.is_file() and config_path.suffix.lower() in {".yaml", ".yml"}:
        paths = [config_path]
    else:
        process_dir = config_path / POST_PROCESS_DIRNAME
        if not process_dir.is_dir():
            return []
        paths = sorted([*process_dir.glob("*.yaml"), *process_dir.glob("*.yml")])

    configs: list[PostProcessConfig] = []
    for path in paths:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"post-process config must contain a mapping: {path}")
        configs.append(PostProcessConfig.from_dict(data))
    return configs


def select_processes(
    processes: list[PostProcessConfig],
    *,
    names: list[str] | None = None,
    tags: list[str] | None = None,
) -> list[PostProcessConfig]:
    selected = processes
    if names:
        wanted = set(names)
        unknown = sorted(wanted - {process.name for process in selected})
        if unknown:
            raise ValueError(
                f"No post-process named {', '.join(unknown)}. "
                f"Available: {', '.join(sorted(p.name for p in processes)) or '(none)'}"
            )
        selected = [process for process in selected if process.name in wanted]
    if tags:
        wanted_tags = set(tags)
        selected = [process for process in selected if wanted_tags.intersection(process.tags)]
    return selected


def filter_process_destinations(
    processes: list[PostProcessConfig], names: list[str]
) -> list[PostProcessConfig]:
    """Restrict each process to the destination(s) named by ``--destination``.

    Mirrors ``cli.filter_destinations`` for loaders, down to the failure modes: the
    filter only ever narrows (a destination disabled in YAML stays skipped even when
    named), a process left with nothing enabled drops out with a note, and a name no
    selected process declares at all raises -- that is a typo, not a narrower run.
    """
    if not names:
        return processes

    wanted = set(names)
    available = {destination.name for process in processes for destination in process.destinations}
    unknown = sorted(wanted - available)
    if unknown:
        raise ValueError(
            f"No destination named {', '.join(unknown)} in the selected post-process(es). "
            f"Available: {', '.join(sorted(available)) or '(none)'}"
        )

    filtered: list[PostProcessConfig] = []
    for process in processes:
        kept = [destination for destination in process.destinations if destination.name in wanted]
        if not any(destination.enabled for destination in kept):
            reason = "declares none of them" if not kept else "has them disabled in config"
            print(
                f"Skipping post-process {process.name}: --destination "
                f"{' '.join(sorted(wanted))} {reason}.",
                file=sys.stderr,
            )
            continue
        filtered.append(replace(process, destinations=kept))
    return filtered


# ── Requirement gate ─────────────────────────────────────────────


def loader_process_map(config_ref: str = "configs") -> dict[str, str]:
    """Map loader ``name`` -> ETLHealth ``ProcessName`` from the loader configs.

    The inverse of what notify builds, and read from the same configs the batch runs
    from, so a requirement naming ``inventory_location`` resolves to the exact
    ProcessName the loader wrote ("Inventory Location") with no second copy to keep
    in sync.
    """
    from .cli import load_loader_configs

    return {loader.name: loader.process_name for loader in load_loader_configs(config_ref)}


def check_requirements(
    requirement: Requirement,
    *,
    destination: ProcessDestination,
    health_table: TableRef,
    process_map: dict[str, str],
    report_date: dt.date,
    logger: logging.Logger,
) -> list[str]:
    """Return a human-readable reason per unmet requirement (empty = clear to run).

    Reads ETLHealth rather than the in-memory results of the batch that just ran, so
    an attached run (``--post-run``) and a detached one (``post-run`` hours later)
    decide identically, and a loader re-run by hand counts the moment it succeeds.
    """
    if not requirement.loaders:
        return []

    from .notify import consolidate, fetch_health_rows

    wanted: dict[str, str] = {}
    for loader_name in requirement.loaders:
        process_name = process_map.get(loader_name)
        if process_name is None:
            raise ValueError(
                f"requires.loaders names {loader_name!r}, which is not a configured loader. "
                f"Use the loader's `name:` from configs/loaders/."
            )
        wanted[loader_name] = process_name

    rows = consolidate(fetch_health_rows(health_table, report_date, sorted(set(wanted.values()))))
    scope_note = ""
    if requirement.scope == "destination":
        rows = [row for row in rows if str(row.get("DBConnection") or "") == destination.server]
        scope_note = f" on {destination.server}"

    logger.info(
        "Requirement gate: %s loader(s) must be %s for %s%s",
        len(wanted),
        STATUS_SUCCESS,
        report_date.isoformat(),
        scope_note,
    )

    unmet: list[str] = []
    for process_name in wanted.values():
        process_rows = [row for row in rows if str(row.get("ProcessName") or "") == process_name]
        if not process_rows:
            unmet.append(f"{process_name}: no ETLHealth row for {report_date.isoformat()}{scope_note}")
            continue
        bad = [row for row in process_rows if str(row.get("TaskStatus") or "") != STATUS_SUCCESS]
        if bad:
            detail = ", ".join(
                f"{row.get('TaskStatus')} on {row.get('DBConnection')}/{row.get('TargetTableType')}"
                for row in bad
            )
            unmet.append(f"{process_name}: {detail}")
        else:
            logger.info("  %s: %s (%s row(s))", process_name, STATUS_SUCCESS, len(process_rows))
    for reason in unmet:
        logger.warning("  UNMET %s", reason)
    return unmet


# ── Step execution ───────────────────────────────────────────────


@dataclass
class StepContext:
    """What a ``python:`` step is handed.

    Carries the live connection (already pointed at the destination's server and
    database), the destination's identity, the run's logger, the step's ``options``
    block, and the run date -- everything a ported notebook needs without reaching
    for its own connection string.
    """

    cnxn: Any
    server: str
    database: str
    schema: str | None
    logger: logging.Logger
    options: dict[str, Any]
    report_date: dt.date

    def engine(self):
        """A SQLAlchemy engine on the same server/database, for ``pd.read_sql_query``.
        The caller disposes it (or uses ``with closing(...)``)."""
        return create_sqlalchemy_engine(self.server, self.database)

    def table(self, table: str, schema: str | None = None) -> TableRef:
        """A :class:`TableRef` on this destination, defaulting to its schema."""
        return TableRef(
            server=self.server,
            database=self.database,
            schema=schema or self.schema or "dbo",
            table=table,
        )


def _resolve_python_step(reference: str) -> Callable[[StepContext], int | None]:
    """Import a ``python:`` step's callable.

    ``bullard.build_search_terms`` resolves inside :data:`PROCESS_PACKAGE`; an
    explicit ``some.module:callable`` is imported as given, so a step can live
    outside the processes package if it ever needs to.
    """
    if ":" in reference:
        module_name, _, attribute = reference.partition(":")
    else:
        module_name, _, attribute = reference.rpartition(".")
    if not module_name or not attribute:
        raise ValueError(
            f"python step {reference!r} must be 'module.callable' (or 'pkg.module:callable')."
        )
    if not module_name.startswith("infor_loader."):
        module_name = f"{PROCESS_PACKAGE}.{module_name}"
    module = importlib.import_module(module_name)
    try:
        function = getattr(module, attribute)
    except AttributeError as exc:
        raise ValueError(f"python step {reference!r}: {module_name} has no {attribute!r}.") from exc
    if not callable(function):
        raise ValueError(f"python step {reference!r}: {attribute!r} is not callable.")
    return function


def _run_step(
    step: ProcessStep,
    destination: ProcessDestination,
    cnxn: Any,
    *,
    report_date: dt.date,
    logger: logging.Logger,
) -> int | None:
    """Execute one step and return its row count, if it reports one."""
    previous_timeout = getattr(cnxn, "timeout", 0)
    if step.timeout_seconds is not None:
        cnxn.timeout = step.timeout_seconds
    try:
        if step.exec_sql:
            cursor = cnxn.cursor()
            try:
                logger.info("  exec: %s", step.exec_sql)
                cursor.execute(step.exec_sql, *step.params)
                # A batch proc that SELECTs or PRINTs leaves the connection busy;
                # drain it so the commit below cannot fail on a result set nobody
                # asked for. Row counts come from the proc's own process_log table.
                _drain_results(cursor)
                cnxn.commit()
            finally:
                cursor.close()
            return None

        function = _resolve_python_step(str(step.python))
        context = StepContext(
            cnxn=cnxn,
            server=destination.server,
            database=destination.database,
            schema=destination.schema,
            logger=logger,
            options=dict(step.options),
            report_date=report_date,
        )
        logger.info("  python: %s", step.python)
        result = function(context)
        return int(result) if isinstance(result, (int, float)) and not isinstance(result, bool) else None
    finally:
        try:
            cnxn.timeout = previous_timeout
        except Exception:  # noqa: BLE001 - restoring the timeout must never mask a step error.
            pass


# ── Results ──────────────────────────────────────────────────────


@dataclass
class StepOutcome:
    step: ProcessStep
    status: str
    row_count: int | None = None
    duration_seconds: int = 0
    error: str | None = None


@dataclass
class DestinationOutcome:
    destination: ProcessDestination
    status: str
    row_count: int | None = None
    steps: list[StepOutcome] = field(default_factory=list)
    error: str | None = None
    unmet: list[str] = field(default_factory=list)
    duration_seconds: int = 0


@dataclass
class PostProcessResult:
    process: str
    process_id: str
    status: str
    row_count: int | None
    duration_seconds: int
    log_file_path: str
    error: str | None = None
    destinations: list[DestinationOutcome] = field(default_factory=list)

    @property
    def blocked(self) -> bool:
        return self.status == STATUS_BLOCKED


# ── Runner ───────────────────────────────────────────────────────


class PostProcessRunner:
    """Run one post-process end to end and write its ETLHealth row(s).

    Deliberately shaped like :class:`~infor_loader.file_loader.FileLoader`: build the
    run logger first (so an unreachable log share cannot crash the job), do the work
    inside a try/except that can never escape, and write ETLHealth from a ``finally``
    so a failure is always recorded. The difference is the rollup -- a loader logs a
    row per step, this logs one row per destination.
    """

    def __init__(
        self,
        config: PostProcessConfig,
        *,
        log_root: str | Path | None = None,
        capture_streams: bool | None = None,
        report_date: dt.date | None = None,
        ignore_gate: bool = False,
        process_map: dict[str, str] | None = None,
        destination_workers: int = DEFAULT_DESTINATION_WORKERS,
    ) -> None:
        self.config = config
        self.log_root = Path(log_root or config.log_root)
        self.report_date = report_date or dt.date.today()
        # How many of THIS process's destinations run at once (see _run_destinations).
        self.destination_workers = max(1, destination_workers)
        # Skip the requirement gate for a deliberate manual rerun (mirrors the
        # loaders' --ignore-download): the operator has decided the upstream data is
        # good enough, e.g. after re-running the failed loader by hand.
        self.ignore_gate = ignore_gate
        self.process_map = process_map or {}
        self.capture_streams = (
            config.capture_streams if capture_streams is None else capture_streams
        )

    def run(self) -> PostProcessResult:
        process_id = uuid.uuid4().hex[:32]
        start_wall = dt.datetime.now()
        start_perf = perf_counter()
        logger, log_file_path, log_note = build_run_logger(
            name=self.config.name,
            process_id=process_id,
            log_root=self.log_root,
            level=self._log_level(),
            to_console=self.config.log_to_console,
        )
        status = STATUS_SUCCESS
        error: str | None = None
        row_count: int | None = None
        outcomes: list[DestinationOutcome] = []

        capture = _capture_streams(logger) if self.capture_streams else nullcontext()
        with capture:
            logger.info(
                "Starting post-process %s (%s) for %s",
                self.config.name,
                process_id,
                self.report_date.isoformat(),
            )
            try:
                outcomes = self._run_destinations(logger)

                status, error = _roll_up(outcomes)
                counts = [
                    outcome.row_count for outcome in outcomes if outcome.row_count is not None
                ]
                row_count = sum(counts) if counts else None
                logger.info("Completed post-process %s; status=%s", self.config.name, status)
            except Exception as exc:  # noqa: BLE001 - a daily job must log every failure.
                status = STATUS_FAILED
                error = f"{exc}\n{traceback.format_exc()}"
                logger.exception("Post-process %s failed", self.config.name)
            finally:
                duration = int(perf_counter() - start_perf)
                self._log_health(
                    process_id=process_id,
                    started_at=start_wall,
                    log_file_path=str(log_file_path),
                    log_note=log_note,
                    outcomes=outcomes,
                    status=status,
                    error=error,
                    duration=duration,
                    logger=logger,
                )

        close_logger(logger)
        return PostProcessResult(
            process=self.config.name,
            process_id=process_id,
            status=status,
            row_count=row_count,
            duration_seconds=int(perf_counter() - start_perf),
            log_file_path=str(log_file_path),
            error=error,
            destinations=outcomes,
        )

    def _run_destinations(self, logger: logging.Logger) -> list[DestinationOutcome]:
        """Run every enabled destination and return their outcomes in config order.

        des1 and des2 are different SERVERS, so there is nothing to contend over --
        each destination opens its own connection, checks its own gate and produces
        its own outcome. Running them together therefore costs nothing and makes a
        process take max(des1, des2) instead of the sum. Concurrency is per
        destination, NOT per process, precisely because processes DO share a server.

        Every destination gets a name-prefixed logger so the single per-run log file
        stays readable when two of them interleave.
        """
        enabled: list[ProcessDestination] = []
        for destination in self.config.destinations:
            if not destination.enabled:
                logger.info("Skipping destination %s (disabled in config)", destination.name)
                continue
            enabled.append(destination)
        if not enabled:
            return []

        # Never more threads than there is work: with one destination configured
        # (des2 not deployed yet) this stays sequential no matter what was asked for.
        workers = min(self.destination_workers, len(enabled))
        if workers <= 1:
            return [
                self._run_destination(destination, _destination_logger(logger, destination))
                for destination in enabled
            ]

        logger.info(
            "Running %s destinations concurrently: %s",
            len(enabled),
            ", ".join(destination.name for destination in enabled),
        )
        collected: list[tuple[int, DestinationOutcome]] = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    self._run_destination, destination, _destination_logger(logger, destination)
                ): index
                for index, destination in enumerate(enabled)
            }
            # A raise propagates to run()'s handler and fails the whole process, but
            # the executor still waits for the other destination on the way out --
            # a batch proc already running is left to finish, not orphaned mid-flight.
            for future in as_completed(futures):
                collected.append((futures[future], future.result()))
        collected.sort(key=lambda item: item[0])
        return [outcome for _, outcome in collected]

    def _run_destination(
        self, destination: ProcessDestination, logger: logging.Logger
    ) -> DestinationOutcome:
        start_perf = perf_counter()
        logger.info("Starting %s", destination.display_name())

        unmet = self._check_gate(destination, logger)
        if unmet and self.config.requires.on_unmet == "block":
            logger.warning(
                "%s: %s. No statement was executed.", STATUS_BLOCKED, "; ".join(unmet)
            )
            return DestinationOutcome(
                destination=destination,
                status=STATUS_BLOCKED,
                unmet=unmet,
                duration_seconds=int(perf_counter() - start_perf),
            )
        if unmet:
            logger.warning(
                "Requirement(s) unmet but on_unmet=warn; running anyway: %s", "; ".join(unmet)
            )

        steps: list[StepOutcome] = []
        error: str | None = None
        cnxn = connect_sql_server(destination.server, destination.database)
        try:
            for index, step in enumerate(destination.steps):
                if error is not None:
                    # An earlier step failed; the rest never started. Recorded so the
                    # log's step summary shows the whole plan, not just what ran.
                    steps.append(StepOutcome(step=step, status=STATUS_NOT_RUN))
                    continue
                step_start = perf_counter()
                logger.info("Step %s/%s: %s", index + 1, len(destination.steps), step.name)
                try:
                    step_rows = _run_step(
                        step,
                        destination,
                        cnxn,
                        report_date=self.report_date,
                        logger=logger,
                    )
                except Exception as exc:  # noqa: BLE001 - recorded, then stops this destination.
                    error = f"step '{step.name}': {exc}\n{traceback.format_exc()}"
                    logger.exception("Step %s failed", step.name)
                    steps.append(
                        StepOutcome(
                            step=step,
                            status=STATUS_FAILED,
                            duration_seconds=int(perf_counter() - step_start),
                            error=error,
                        )
                    )
                    continue
                step_duration = int(perf_counter() - step_start)
                logger.info(
                    "Step %s finished in %ss%s",
                    step.name,
                    step_duration,
                    "" if step_rows is None else f" (rows={step_rows})",
                )
                steps.append(
                    StepOutcome(
                        step=step,
                        status=STATUS_SUCCESS,
                        row_count=step_rows,
                        duration_seconds=step_duration,
                    )
                )
        finally:
            cnxn.close()

        counts = [outcome.row_count for outcome in steps if outcome.row_count is not None]
        _log_step_summary(steps, logger)
        return DestinationOutcome(
            destination=destination,
            status=STATUS_FAILED if error else STATUS_SUCCESS,
            row_count=sum(counts) if counts else None,
            steps=steps,
            error=error,
            duration_seconds=int(perf_counter() - start_perf),
        )

    def _check_gate(
        self, destination: ProcessDestination, logger: logging.Logger
    ) -> list[str]:
        if self.ignore_gate:
            if self.config.requires.loaders:
                logger.warning(
                    "Requirement gate SKIPPED (--ignore-gate); not checking %s",
                    ", ".join(self.config.requires.loaders),
                )
            return []
        if self.config.health_table is None:
            # Nothing to read the upstream statuses from. Fail loudly rather than
            # silently running a process whose preconditions were never checked.
            if self.config.requires.loaders:
                raise ValueError(
                    f"{self.config.name}: requires.loaders is set but no health_table is "
                    "configured to check them against."
                )
            return []
        return check_requirements(
            self.config.requires,
            destination=destination,
            health_table=self.config.health_table,
            process_map=self.process_map,
            report_date=self.report_date,
            logger=logger,
        )

    def _log_health(
        self,
        *,
        process_id: str,
        started_at: dt.datetime,
        log_file_path: str,
        log_note: str | None,
        outcomes: list[DestinationOutcome],
        status: str,
        error: str | None,
        duration: int,
        logger: logging.Logger,
    ) -> None:
        """Write ONE ETLHealth row per destination: the top-level verdict only.

        The sub-procedure detail the two batch procs record in their own process_log
        tables is not duplicated here; this row points at the log file, which carries
        the per-step breakdown and the failing traceback.
        """
        health_table = self.config.health_table
        if health_table is None:
            logger.warning("No health_table configured; skipping ETLHealth insert.")
            return

        rows = outcomes or [
            # The run blew up before any destination was attempted (e.g. a bad
            # requirement name): still record the failure against each enabled one.
            DestinationOutcome(destination=destination, status=status, error=error, duration_seconds=duration)
            for destination in self.config.destinations
            if destination.enabled
        ]

        try:
            cnxn = connect_sql_server(health_table.server, health_table.database)
            try:
                for outcome in rows:
                    destination = outcome.destination
                    payload = {
                        "ProcessName": self.config.process_name,
                        "ProcessID": process_id,
                        "SourceFilePath": None,
                        "LastRunTime": started_at,
                        "TargetTableName": _fit(
                            "; ".join(step.target_label(destination) for step in destination.steps),
                            _MAX_TARGET_TABLE_NAME,
                        ),
                        "TaskStatus": outcome.status,
                        "RowCount": outcome.row_count,
                        "PackagePath": str(Path.cwd()),
                        "LogFilePath": log_file_path,
                        # No staging table feeds a procedure run.
                        "STGTableName": "Not Applicable",
                        "ProcessFrequency": self.config.process_frequency,
                        "Error": _health_error(outcome, log_note),
                        "Duration": outcome.duration_seconds or duration,
                        "DBConnection": destination.server,
                        "ProcessType": _process_type(destination),
                        "TargetTableType": TARGET_TABLE_TYPE,
                    }
                    insert_health_record(cnxn, health_table, payload)
            finally:
                cnxn.close()
        except Exception:  # noqa: BLE001 - health logging must not hide the run's own result.
            logger.exception("Failed to write ETLHealth record")

    def _log_level(self) -> int:
        level = logging.getLevelName(str(self.config.log_level).upper())
        return level if isinstance(level, int) else logging.INFO


def run_post_processes(
    processes: list[PostProcessConfig],
    *,
    config_ref: str = "configs",
    log_root: str | None = None,
    report_date: dt.date | None = None,
    ignore_gate: bool = False,
    max_workers: int = 1,
    destination_workers: int = DEFAULT_DESTINATION_WORKERS,
) -> list[PostProcessResult]:
    """Run each selected post-process and return one result apiece.

    Two INDEPENDENT concurrency axes, which multiply -- at most
    ``max_workers x destination_workers`` procedures run at once:

    * ``max_workers`` -- how many PROCESSES overlap. Defaults to 1 because the batch
      procs are heavy and PLM/Preprocessor share a server, so overlapping them buys
      little and costs contention.
    * ``destination_workers`` -- how many DESTINATIONS of one process overlap.
      Defaults to 2 because des1 and des2 are different servers with nothing to
      contend over.

    The defaults therefore put exactly one procedure on each server at a time, which
    is the shape you want; raising both is what would stack several onto one server.
    """
    # Built once for the whole batch: every process's gate resolves loader names
    # against the same map instead of re-parsing every loader YAML per process.
    process_map = loader_process_map(config_ref)

    def make(process: PostProcessConfig, *, capture: bool | None) -> PostProcessRunner:
        return PostProcessRunner(
            process,
            log_root=log_root,
            capture_streams=capture,
            report_date=report_date,
            ignore_gate=ignore_gate,
            process_map=process_map,
            destination_workers=destination_workers,
        )

    if max_workers <= 1:
        return [make(process, capture=None).run() for process in processes]

    results: list[PostProcessResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(make(process, capture=False).run): process for process in processes}
        for future in as_completed(futures):
            results.append(future.result())
    order = [process.name for process in processes]
    results.sort(key=lambda result: order.index(result.process))
    return results


# ── Helpers ──────────────────────────────────────────────────────


def _roll_up(outcomes: list[DestinationOutcome]) -> tuple[str, str | None]:
    """The process-level status across its destinations, with the reason.

    Any failure wins. Otherwise, BLOCKED only when *nothing* ran -- a run where des1
    worked and des2 was blocked is a success that skipped a side, not a blocked run.
    """
    if not outcomes:
        return STATUS_SUCCESS, None
    failures = [outcome for outcome in outcomes if outcome.status == STATUS_FAILED]
    if failures:
        return STATUS_FAILED, "\n\n".join(
            outcome.error or f"{outcome.destination.name} failed." for outcome in failures
        )
    blocked = [outcome for outcome in outcomes if outcome.status == STATUS_BLOCKED]
    if len(blocked) == len(outcomes):
        return STATUS_BLOCKED, "; ".join(reason for outcome in blocked for reason in outcome.unmet)
    return STATUS_SUCCESS, None


def _health_error(outcome: DestinationOutcome, log_note: str | None) -> str | None:
    """Value for the ETLHealth ``Error`` column of one destination's row.

    A failure gets the classified code (the traceback is in the log file); a blocked
    destination names what it is waiting on, since that IS the actionable detail and
    appears nowhere else; a success carries only the log-fallback note, if any.
    """
    if outcome.status == STATUS_FAILED:
        return classify_error(outcome.error)
    if outcome.status == STATUS_BLOCKED:
        return _fit("BLOCKED: requires " + "; ".join(outcome.unmet), _MAX_ERROR)
    return log_note or None


def _process_type(destination: ProcessDestination) -> str:
    """ETLHealth ProcessType: what this destination's steps actually run."""
    kinds = {step.kind for step in destination.steps}
    if len(kinds) == 1:
        return next(iter(kinds))
    return "mixed"


class _DestinationLogAdapter(logging.LoggerAdapter):
    """Tag every line with the destination it came from.

    Destinations share one per-run log file, so once two of them run concurrently an
    untagged ``Step 1/2: daily_archive`` is ambiguous. The prefix is applied even
    when running sequentially, so the log format does not change shape depending on
    how the run was invoked.
    """

    def process(self, msg: Any, kwargs: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
        return f"[{self.extra['destination']}] {msg}", kwargs


def _destination_logger(
    logger: logging.Logger, destination: ProcessDestination
) -> logging.LoggerAdapter:
    return _DestinationLogAdapter(logger, {"destination": destination.name})


def _log_step_summary(steps: list[StepOutcome], logger: logging.Logger) -> None:
    """Write the per-step breakdown the single ETLHealth row deliberately omits."""
    logger.info("Step summary:")
    for outcome in steps:
        rows = "" if outcome.row_count is None else f" rows={outcome.row_count}"
        logger.info(
            "  %-8s %-28s %ss%s", outcome.status, outcome.step.name, outcome.duration_seconds, rows
        )


def _drain_results(cursor: Any) -> None:
    """Consume any result sets a statement returned so the connection can commit."""
    try:
        while cursor.nextset():
            pass
    except Exception:  # noqa: BLE001 - nothing left to drain is the normal case.
        pass


def _fit(value: str | None, limit: int) -> str | None:
    """Trim a value to an ETLHealth column's width, marking that it was cut."""
    if value is None:
        return None
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _object_name_from_exec(statement: str) -> str | None:
    """The procedure name an ``EXEC``/``EXECUTE`` statement calls, or None."""
    text = statement.strip()
    lowered = text.lower()
    for keyword in ("execute ", "exec "):
        if lowered.startswith(keyword):
            rest = text[len(keyword):].strip()
            name = ""
            for char in rest:
                if char in " \t\r\n(;":
                    break
                name += char
            return name or None
    return None


def _qualify_object(raw: str, destination: ProcessDestination) -> str:
    """``[database].[schema].[object]`` for a target named in config or read off an
    EXEC. A bare name takes the destination's schema; a ``schema.object`` keeps its
    own. Already-bracketed parts are unwrapped before re-bracketing so the result is
    escaped exactly once."""
    parts = [part.strip().strip("[]") for part in raw.split(".")]
    parts = [part for part in parts if part]
    if not parts:
        return raw
    if len(parts) == 1:
        parts = [destination.schema or "dbo", *parts]
    if len(parts) == 2:
        parts = [destination.database, *parts]
    return ".".join(bracket_identifier(part) for part in parts[-3:])
