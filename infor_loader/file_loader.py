from __future__ import annotations

import logging
import shutil
import sys
import traceback
import uuid
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd

from .config import AuxStaging, LoadDestination, LoaderConfig, OverlapCheck, TableRef
from .file_folder import InputFile, run_moves
from .db import (
    connect_sql_server,
    count_rows,
    execute_statements,
    get_insert_columns,
    get_max_value,
    insert_dataframe,
    insert_health_record,
    truncate_table,
)
from .transforms import TRANSFORMS, apply_type_changes, move_files, normalize_for_db


@dataclass
class LoaderResult:
    loader_name: str
    process_id: str
    status: str
    row_count: int | None
    duration_seconds: int
    log_file_path: str
    error: str | None = None


@dataclass
class StepResult:
    """One logged unit of work for a destination (staging load or prod update)."""

    step: str
    target: TableRef
    staging_table: TableRef
    status: str
    row_count: int | None
    error: str | None = None
    duration: int | None = None
    # Extra staging tables to name alongside ``staging_table`` in the ETLHealth
    # STGTableName column. Set on a prod step to list the auxiliary staging tables
    # that also feed the promotion, so the PRD row shows every staging source.
    extra_staging_tables: tuple[TableRef, ...] = ()
    # ETLHealth TargetTableType for this step's table. A normal staging step is
    # "STG"; a prod-promotion step is "PRD". A direct-load destination (no prod
    # table configured) types its single load step "PRD" too, because the table
    # it truncates/loads IS the final production table.
    table_type: str = "STG"


@dataclass
class DestinationLoadResult:
    destination: LoadDestination
    status: str
    row_count: int | None
    error: str | None = None
    steps: list[StepResult] = field(default_factory=list)


class FileLoader:
    #: Run only the file->staging load, only the staging->prod promotion, or both.
    PHASES = frozenset({"both", "stg", "prd"})

    def __init__(
        self,
        config: LoaderConfig,
        *,
        log_root: str | Path | None = None,
        capture_streams: bool | None = None,
        phase: str = "both",
        download_inputs: list[InputFile] | None = None,
        ignore_download: bool = False,
    ) -> None:
        if phase not in self.PHASES:
            raise ValueError(f"phase must be one of {sorted(self.PHASES)}; got {phase!r}.")
        self.config = config
        self.log_root = Path(log_root or config.log_root)
        # "both" = file->staging then staging->prod; "stg" = staging load only
        # (skip prod promotion); "prd" = promotion only (skip the file/staging load).
        self.phase = phase
        # The loader's downloadable inputs (from the file-folder registry). Unless
        # ignore_download is set, the run gates on a fresh download for every one
        # of these before touching the DB; an empty list disables the gate.
        self.download_inputs = list(download_inputs or [])
        self.ignore_download = ignore_download
        # Stream capture redirects the process-global stdout/stderr, so callers
        # running loaders in parallel can force it off to avoid cross-talk.
        self.capture_streams = (
            config.capture_streams if capture_streams is None else capture_streams
        )

    def run(self) -> LoaderResult:
        process_id = uuid.uuid4().hex[:32]
        start_wall = datetime.now()
        start_perf = perf_counter()
        logger, log_file_path, log_note = self._build_logger(process_id)
        row_count: int | None = None
        status = "SUCCESS"
        error: str | None = None
        source_paths: list[str] = []
        # Non-fatal warning (e.g. an ignored source column is absent) surfaced on
        # the ETLHealth staging row even when the run succeeds.
        source_warning: str | None = None
        destination_results: list[DestinationLoadResult] = []

        capture = _capture_streams(logger) if self.capture_streams else nullcontext()
        with capture:
            logger.info("Starting loader %s (%s) [phase=%s]", self.config.name, process_id, self.phase)
            try:
                # --prd-only promotes the already-loaded staging table, so the source
                # file is neither moved nor read.
                df: pd.DataFrame | None = None
                # Prepared secondary-staging frames (source-named, type-converted),
                # loaded into each destination before prod promotion. Empty unless the
                # loader defines aux_stagings, and never populated for --prd-only.
                aux_frames: list[tuple[AuxStaging, pd.DataFrame]] = []

                # Download gate: unless --ignore-download, require a fresh export in
                # Downloads for EVERY downloadable input before touching the DB. If any
                # is missing, record FILE_NOT_FOUND and skip the load entirely -- never
                # re-truncate prod with the stale copy already sitting in place. Skipped
                # for --prd-only, which reads no source file.
                missing_downloads: list[str] = []
                if self.phase != "prd" and not self.ignore_download and self.download_inputs:
                    missing_downloads = self._check_and_stage_downloads(logger)

                if missing_downloads:
                    status = "FILE_NOT_FOUND"
                    error = "no files matched in Downloads for input(s): " + ", ".join(missing_downloads)
                    logger.warning(
                        "%s; skipping loader %s (no fresh download; prod left untouched).",
                        error,
                        self.config.name,
                    )
                else:
                    if self.phase != "prd":
                        if self.config.pre_file_moves:
                            move_files(self.config.pre_file_moves, logger)
                        dataframes, source_paths = self._read_sources(logger)
                        df, source_warning = self._prepare_dataframe(dataframes, logger)
                        aux_frames = self._prepare_aux_frames(dataframes, logger)

                    for destination in self.config.destinations:
                        if not destination.enabled:
                            logger.info("Skipping destination %s (disabled in config)", destination.name)
                            continue
                        destination_results.append(self._load_destination(destination, df, aux_frames, logger))

                    failures = [result for result in destination_results if result.status != "SUCCESS"]
                    if failures:
                        status = "FAILED"
                        error = "\n\n".join(result.error or f"{result.destination.name} failed." for result in failures)
                    else:
                        row_count = sum(result.row_count or 0 for result in destination_results)

                    # Copy the consumed source file into the archive folder (stamped
                    # with the run date) once it has been successfully loaded. Skipped
                    # for --prd-only, which reads no file.
                    if status == "SUCCESS" and self.phase != "prd" and self.config.archive_dir:
                        self._archive_sources(source_paths, start_wall, logger)

                    # Archive/move the consumed source only after the full pipeline
                    # (staging + prod) succeeds; a partial --stg-only run leaves the file
                    # in place so it can still be promoted or re-run.
                    if status == "SUCCESS" and self.phase == "both" and self.config.post_file_moves:
                        move_files(self.config.post_file_moves, logger)

                    logger.info("Completed loader %s; rows=%s", self.config.name, row_count)
            except Exception as exc:  # noqa: BLE001 - the loader must log every daily-job failure.
                status = "FAILED"
                error = f"{exc}\n{traceback.format_exc()}"
                logger.exception("Loader %s failed", self.config.name)
            finally:
                duration = int(perf_counter() - start_perf)
                self._log_health(
                    process_id=process_id,
                    source_paths=source_paths,
                    started_at=start_wall,
                    status=status,
                    row_count=row_count,
                    log_file_path=str(log_file_path),
                    error=error,
                    duration=duration,
                    destination_results=destination_results,
                    source_warning=source_warning,
                    log_note=log_note,
                    logger=logger,
                )

        self._close_logger(logger)

        return LoaderResult(
            loader_name=self.config.name,
            process_id=process_id,
            status=status,
            row_count=row_count,
            duration_seconds=int(perf_counter() - start_perf),
            log_file_path=str(log_file_path),
            error=error,
        )

    def _check_and_stage_downloads(self, logger: logging.Logger) -> list[str]:
        """Gate the loader on a fresh download for EVERY downloadable input, then
        stage the ones that are present.

        Two phases, so nothing is consumed unless the loader will actually run:
          1. probe Downloads with ``run_moves(dry_run=True)`` -- touches no file;
             any input reporting NO_MATCH/ERROR has no fresh download.
          2. only if none are missing, ``run_moves(dry_run=False)`` relocates each
             fresh export into its designated folder (the loader's source location),
             so ``_read_sources`` then picks up today's file.

        Returns the input keys with no fresh file (empty list = all present, staged).
        """
        probe = run_moves(self.download_inputs, dry_run=True, logger=logger)
        missing = [result.key for result in probe if result.status in {"NO_MATCH", "ERROR"}]
        if missing:
            return missing
        run_moves(self.download_inputs, dry_run=False, logger=logger)
        return []

    def _read_sources(self, logger: logging.Logger) -> tuple[dict[str, pd.DataFrame], list[str]]:
        dataframes: dict[str, pd.DataFrame] = {}
        source_paths: list[str] = []
        for source in self.config.source_files:
            file_path = source.resolve()
            source_paths.append(str(file_path))
            logger.info("Reading %s source %s", source.reader, file_path)
            if source.reader == "csv":
                df = pd.read_csv(file_path, **source.options)
            elif source.reader == "excel":
                df = pd.read_excel(file_path, **source.options)
            else:
                raise ValueError(f"Unsupported reader {source.reader!r} for {file_path}")
            dataframes[source.alias] = df
        return dataframes, source_paths

    def _archive_sources(
        self,
        source_paths: list[str],
        run_time: datetime,
        logger: logging.Logger,
    ) -> None:
        """Copy each consumed source file into the archive folder, appending a
        ``_YYYYMMDD`` stamp to the name and keeping the original extension.

        Archiving is a best-effort side task: a failure here is logged but never
        fails the loader, since the data is already loaded.
        """
        archive_dir = Path(self.config.archive_dir)
        stamp = run_time.strftime("%Y%m%d")
        try:
            archive_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            logger.exception("Could not create archive directory %s; skipping archive.", archive_dir)
            return
        for raw_path in source_paths:
            source = Path(raw_path)
            target = archive_dir / f"{source.stem}_{stamp}{source.suffix}"
            try:
                shutil.copy2(source, target)
                logger.info("Archived source %s -> %s", source, target)
            except OSError:
                logger.exception("Failed to archive source %s to %s", source, target)

    def _prepare_dataframe(
        self, dataframes: dict[str, pd.DataFrame], logger: logging.Logger
    ) -> tuple[pd.DataFrame, str | None]:
        primary_alias = self.config.source_files[0].alias
        df = dataframes[primary_alias].copy()

        rename_columns = self.config.rename_columns
        if rename_columns:
            df = df.rename(columns=rename_columns)

        source_warning = self._validate_expected_source_columns(df, logger)

        df = apply_type_changes(
            df,
            self.config.type_changes,
            date_format=self.config.date_format,
            datetime_format=self.config.datetime_format,
            datetime_to_date=self.config.datetime_to_date_columns,
        )

        drop_columns = [column for column in self.config.drop_columns if column in df.columns]
        if drop_columns:
            df = df.drop(columns=drop_columns)

        for transform_name in self.config.transforms:
            transform = TRANSFORMS.get(transform_name)
            if transform is None:
                raise ValueError(f"Unknown transform {transform_name!r}")
            logger.info("Applying transform %s", transform_name)
            df = transform(df, self.config, dataframes, logger)

        destination_columns = self.config.destination_columns
        if destination_columns:
            missing = [column for column in destination_columns if column not in df.columns]
            if missing:
                raise ValueError(f"{self.config.name} is missing destination columns after transform: {missing}")
            df = df[destination_columns].copy()

        df = normalize_for_db(
            df,
            self.config.type_changes,
            preserve_whitespace=self.config.preserve_whitespace_columns,
        )
        return df, source_warning

    def _load_destination(
        self,
        destination: LoadDestination,
        df: pd.DataFrame | None,
        aux_frames: list[tuple[AuxStaging, pd.DataFrame]],
        logger: logging.Logger,
    ) -> DestinationLoadResult:
        logger.info("Loading destination %s", destination.display_name(include_server=True))
        steps: list[StepResult] = []
        row_count: int | None = None
        # A direct destination's load step targets the final table, not a staging table.
        load_table_type = "PRD" if destination.is_direct else "STG"
        # Aux staging tables this destination promotes from, in aux_stagings order.
        # Named on the prod ETLHealth row's STGTableName alongside the primary
        # staging table so the PRD row shows every staging source that feeds it.
        aux_targets = tuple(
            destination.aux_staging[aux.name]
            for aux, _ in aux_frames
            if aux.name in destination.aux_staging
        )

        # Step 1: load the staging table (skipped for --prd-only, which promotes
        # the staging data already loaded by a prior run).
        if self.phase != "prd":
            staging_start = perf_counter()
            try:
                row_count = self._load_staging(destination, df, logger)
            except Exception as exc:  # noqa: BLE001 - keep loading other configured destinations.
                error = f"{exc}\n{traceback.format_exc()}"
                logger.exception("Destination %s staging load failed", destination.name)
                steps.append(
                    StepResult(
                        step="staging",
                        target=destination.staging,
                        staging_table=destination.staging,
                        status="FAILED",
                        row_count=None,
                        error=error,
                        duration=int(perf_counter() - staging_start),
                        table_type=load_table_type,
                    )
                )
                # In a full run (phase "both"), the prod promotion that would have
                # followed cannot run once staging failed. Record its PRD ETLHealth row
                # as FAILED with a fixed "STAGING LOAD FAILED" marker, so the skipped
                # prod job is visible as failed rather than silently absent. A
                # destination with no prod promotion (no prod.post_sql) has no PRD job
                # to log.
                if self.phase == "both" and destination.prod_post_sql:
                    steps.append(
                        StepResult(
                            step="prod",
                            target=destination.prod or destination.staging,
                            staging_table=destination.staging,
                            status="FAILED",
                            row_count=None,
                            error="STAGING LOAD FAILED",
                            duration=0,
                            table_type="PRD",
                            extra_staging_tables=aux_targets,
                        )
                    )
                return DestinationLoadResult(destination, "FAILED", None, error, steps)

            logger.info("Loaded %s rows to %s", row_count, destination.staging.display_name(include_server=True))
            steps.append(
                StepResult(
                    step="staging",
                    target=destination.staging,
                    staging_table=destination.staging,
                    status="SUCCESS",
                    row_count=row_count,
                    duration=int(perf_counter() - staging_start),
                    table_type=load_table_type,
                )
            )

            # Step 1b: load each auxiliary staging table (a secondary file feeding a
            # purpose-built table this destination's prod.post_sql reads, e.g. the
            # requisition PO-source merge). Runs on the destination connection AFTER
            # the primary staging load and BEFORE prod promotion. A failure here is
            # treated like a primary staging failure: the prod promotion must not run
            # against a stale/empty aux table, so it is recorded FAILED and skipped.
            for aux, aux_source_df in aux_frames:
                # Resolved per destination from staging.aux (config guarantees every
                # enabled destination declares a table for each aux staging).
                aux_table = destination.aux_staging[aux.name]
                aux_start = perf_counter()
                try:
                    aux_rows = self._load_aux_staging(aux, aux_table, aux_source_df, logger)
                except Exception as exc:  # noqa: BLE001 - keep loading other destinations.
                    error = f"{exc}\n{traceback.format_exc()}"
                    logger.exception(
                        "Destination %s aux staging load into %s failed", destination.name, aux_table.table
                    )
                    steps.append(
                        StepResult(
                            step="staging",
                            target=aux_table,
                            staging_table=aux_table,
                            status="FAILED",
                            row_count=None,
                            error=error,
                            duration=int(perf_counter() - aux_start),
                            table_type="STG",
                        )
                    )
                    if self.phase == "both" and destination.prod_post_sql:
                        steps.append(
                            StepResult(
                                step="prod",
                                target=destination.prod or destination.staging,
                                staging_table=destination.staging,
                                status="FAILED",
                                row_count=None,
                                error="STAGING LOAD FAILED",
                                duration=0,
                                table_type="PRD",
                                extra_staging_tables=aux_targets,
                            )
                        )
                    return DestinationLoadResult(destination, "FAILED", None, error, steps)

                logger.info(
                    "Loaded %s rows to aux staging %s",
                    aux_rows,
                    aux_table.display_name(include_server=True),
                )
                steps.append(
                    StepResult(
                        step="staging",
                        target=aux_table,
                        staging_table=aux_table,
                        status="SUCCESS",
                        row_count=aux_rows,
                        duration=int(perf_counter() - aux_start),
                        table_type="STG",
                    )
                )

        # Step 2: promote staging into prod via the configured prod statements
        # (skipped for --stg-only).
        if self.phase == "stg":
            return DestinationLoadResult(destination, "SUCCESS", row_count, None, steps)

        if self.phase == "prd" and not destination.prod_post_sql:
            logger.warning(
                "Destination %s has no prod.post_sql; nothing to promote for --prd-only.",
                destination.name,
            )

        if destination.prod_post_sql:
            prod_target = destination.prod or destination.staging
            prod_start = perf_counter()
            try:
                self._update_prod(destination, prod_target, logger)
            except Exception as exc:  # noqa: BLE001 - keep loading other configured destinations.
                error = f"{exc}\n{traceback.format_exc()}"
                logger.exception("Destination %s prod update failed", destination.name)
                steps.append(
                    StepResult(
                        step="prod",
                        target=prod_target,
                        staging_table=destination.staging,
                        status="FAILED",
                        row_count=None,
                        error=error,
                        duration=int(perf_counter() - prod_start),
                        table_type="PRD",
                        extra_staging_tables=aux_targets,
                    )
                )
                return DestinationLoadResult(destination, "FAILED", row_count, error, steps)

            logger.info("Updated prod %s from staging", prod_target.display_name(include_server=True))
            steps.append(
                StepResult(
                    step="prod",
                    target=prod_target,
                    staging_table=destination.staging,
                    status="SUCCESS",
                    row_count=row_count,
                    duration=int(perf_counter() - prod_start),
                    table_type="PRD",
                    extra_staging_tables=aux_targets,
                )
            )

        return DestinationLoadResult(destination, "SUCCESS", row_count, None, steps)

    def _load_staging(
        self,
        destination: LoadDestination,
        df: pd.DataFrame,
        logger: logging.Logger,
    ) -> int:
        # Delta-overlap guard runs before the truncate/insert so a gap in the
        # incoming file aborts this destination without touching stored data.
        self._check_overlap(destination, df, logger)

        staging_table = destination.staging
        cnxn = connect_sql_server(staging_table.server, staging_table.database)
        try:
            output_df, insert_columns = self._align_for_destination(cnxn, df, staging_table, logger)
            self._validate_pk(output_df, logger)
            if self.config.stg_load_strategy == "truncate_insert":
                row_count = self._truncate_insert(cnxn, staging_table, output_df, insert_columns)
            else:
                raise ValueError(f"Unsupported stg_load strategy: {self.config.stg_load_strategy}")

            statements = [*self.config.post_sql, *destination.post_sql]
            if statements:
                execute_statements(cnxn, statements)
            return row_count
        finally:
            cnxn.close()

    def _truncate_insert(
        self,
        cnxn: Any,
        table: TableRef,
        output_df: pd.DataFrame,
        insert_columns: list[str],
    ) -> int:
        """Truncate ``table`` and reload it from ``output_df``, with the partial-load
        guardrails shared by the primary and auxiliary staging loads."""
        truncate_table(cnxn, table)
        expected_rows = len(output_df)
        try:
            row_count = insert_dataframe(
                cnxn,
                table,
                output_df,
                insert_columns,
                batch_size=self.config.batch_size,
            )
        except Exception as exc:
            # The insert commits per batch, so an interrupted load leaves the
            # (already truncated) staging table holding only the earlier batches.
            # Surface that partial state explicitly in the error.
            committed = _committed_row_count(cnxn, table)
            committed_text = "an unknown number of" if committed is None else str(committed)
            raise RuntimeError(
                f"PARTIAL STAGING LOAD: insert into "
                f"{table.display_name(include_server=True)} stopped mid-load with "
                f"{committed_text} of {expected_rows} rows committed; the table was "
                f"truncated at the start of the run, so it is incomplete until the next "
                f"successful load."
            ) from exc
        # Guard against a silent partial load: everything the insert reported must
        # actually be in the table before post_sql or prod promotion runs.
        landed_rows = count_rows(cnxn, table)
        if landed_rows != expected_rows:
            raise RuntimeError(
                f"PARTIAL STAGING LOAD: "
                f"{table.display_name(include_server=True)} holds {landed_rows} "
                f"rows after the load but the prepared source has {expected_rows}; "
                f"failing so the incomplete staging data is not promoted."
            )
        return row_count

    def _prepare_aux_frames(
        self, dataframes: dict[str, pd.DataFrame], logger: logging.Logger
    ) -> list[tuple[AuxStaging, pd.DataFrame]]:
        """Prepare each aux staging's frame from its source file: rename (if any),
        type-convert, and normalize -- the same pre-mapping steps the primary
        pipeline runs. The source->destination column mapping is applied later, per
        destination, in ``_load_aux_staging`` (mirroring how the primary frame is
        mapped at align time). One prepared frame is reused for every destination."""
        frames: list[tuple[AuxStaging, pd.DataFrame]] = []
        for aux in self.config.aux_stagings:
            if aux.source_alias not in dataframes:
                raise ValueError(
                    f"{self.config.name} aux staging '{aux.name}' references source alias "
                    f"{aux.source_alias!r} not found among source files: {sorted(dataframes)}"
                )
            logger.info("Preparing aux staging frame %r from source %r", aux.name, aux.source_alias)
            frame = dataframes[aux.source_alias].copy()
            if aux.rename_columns:
                frame = frame.rename(columns=aux.rename_columns)
            frame = apply_type_changes(
                frame,
                aux.type_changes,
                date_format=aux.date_format,
                datetime_format=aux.datetime_format,
                datetime_to_date=aux.datetime_to_date_columns,
            )
            frame = normalize_for_db(frame, aux.type_changes)
            frames.append((aux, frame))
        return frames

    def _load_aux_staging(
        self,
        aux: AuxStaging,
        aux_table: TableRef,
        source_df: pd.DataFrame,
        logger: logging.Logger,
    ) -> int:
        """Map the prepared aux frame to its destination columns and truncate/insert
        it into ``aux_table`` (this destination's copy of the aux staging table)."""
        cnxn = connect_sql_server(aux_table.server, aux_table.database)
        try:
            output_df = self._apply_column_mapping(source_df, aux.column_mapping)
            insert_columns = list(output_df.columns)
            self._validate_pk(output_df, logger, pk_columns=aux.pk_check)
            return self._truncate_insert(cnxn, aux_table, output_df, insert_columns)
        finally:
            cnxn.close()

    def _update_prod(
        self,
        destination: LoadDestination,
        prod_target: TableRef,
        logger: logging.Logger,
    ) -> None:
        cnxn = connect_sql_server(prod_target.server, prod_target.database)
        try:
            logger.info(
                "Running %s prod update statement(s) on %s",
                len(destination.prod_post_sql),
                prod_target.display_name(include_server=True),
            )
            execute_statements(cnxn, destination.prod_post_sql)
        finally:
            cnxn.close()

    def _validate_expected_source_columns(
        self, df: pd.DataFrame, logger: logging.Logger
    ) -> str | None:
        """Check that expected source columns are present.

        A missing column is fatal when the pipeline needs it: either it is loaded to a
        destination (has a destination and is not ``ignore``) or it is an explicit
        transform input (``required_input``). A missing column that is only present-but-
        unintegrated ('extra' / no destination) is a non-fatal warning: the run
        continues and the warning is returned so it can be logged on ETLHealth.
        """
        required_missing: list[str] = []
        ignored_missing: list[str] = []
        for item in self.config.column_mapping or []:
            if not _truthy_indicator(item.get("expected_in_source")):
                continue
            source = item.get("source")
            if source is None or source in df.columns:
                continue
            destination = item.get("destination") or item.get("dest")
            # Required if it is loaded to a destination OR marked as a transform input;
            # either being absent means the pipeline cannot produce correct output.
            is_required = (
                (bool(destination) and item.get("type") != "ignore")
                or _truthy_indicator(item.get("required_input"))
            )
            (required_missing if is_required else ignored_missing).append(source)

        source_warning: str | None = None
        if ignored_missing:
            cols = ", ".join(sorted(set(ignored_missing)))
            source_warning = (
                f"COLUMN NOT FOUND (warning): expected file column(s) {cols} missing; "
                f"not loaded, ETL continued."
            )
            logger.warning(source_warning)

        if required_missing:
            missing = sorted(set(required_missing))
            raise ValueError(f"{self.config.name} source file is missing expected columns: {missing}")

        return source_warning

    def _validate_pk(
        self,
        df: pd.DataFrame,
        logger: logging.Logger,
        pk_columns: list[str] | None = None,
    ) -> None:
        pk_columns = self.config.pk_check if pk_columns is None else pk_columns
        if not pk_columns:
            return
        missing = [column for column in pk_columns if column not in df.columns]
        if missing:
            raise ValueError(f"{self.config.name} pk_check columns missing from dataframe: {missing}")

        duplicates = df.groupby(pk_columns, dropna=False).size().reset_index(name="count")
        duplicates = duplicates[duplicates["count"] > 1].sort_values("count", ascending=False)
        if duplicates.empty:
            return

        sample = duplicates.head(20).to_dict(orient="records")
        logger.warning("PK duplicate check found %s duplicate keys; sample=%s", len(duplicates), sample)
        if self.config.fail_on_pk_duplicates:
            raise ValueError(f"{self.config.name} failed pk_check; see log for duplicate key sample.")

    def _check_overlap(
        self,
        destination: LoadDestination,
        df: pd.DataFrame,
        logger: logging.Logger,
    ) -> None:
        """Guard a rolling-window delta file against leaving a hole in the data.

        Asserts the incoming file overlaps what is already stored:
        ``MIN(source[source_column]) <= MAX(baseline[db_column])``. Runs before the
        staging truncate/insert, so a gap aborts (or warns for) this destination
        without mutating any table.
        """
        check = self.config.overlap_check
        if check is None or not check.enabled:
            return

        if check.source_column not in df.columns:
            raise ValueError(
                f"{self.config.name} overlap_check.source_column {check.source_column!r} "
                f"is not in the prepared source columns: {list(df.columns)}"
            )
        source_values = df[check.source_column].dropna()
        source_min = source_values.min() if not source_values.empty else None
        if source_min is None or pd.isna(source_min):
            self._handle_overlap_failure(
                check,
                f"Overlap check for {destination.name}: source column "
                f"{check.source_column!r} has no non-null values to compare.",
                logger,
            )
            return

        baseline_table = destination.prod if check.baseline == "prod" else destination.staging
        if baseline_table is None:
            logger.warning(
                "Overlap check baseline 'prod' requested but destination %s has no prod "
                "table; reading MAX(%s) from staging instead.",
                destination.name,
                check.db_column,
            )
            baseline_table = destination.staging

        cnxn = connect_sql_server(baseline_table.server, baseline_table.database)
        try:
            db_max = get_max_value(cnxn, baseline_table, check.db_column)
        finally:
            cnxn.close()

        if db_max is None:
            if check.skip_if_db_empty:
                logger.info(
                    "Overlap check for %s: baseline %s has no %s values yet "
                    "(empty/first load); skipping.",
                    destination.name,
                    baseline_table.display_name(),
                    check.db_column,
                )
                return
            self._handle_overlap_failure(
                check,
                f"Overlap check for {destination.name}: baseline "
                f"{baseline_table.display_name()} has no {check.db_column!r} values and "
                f"skip_if_db_empty is false.",
                logger,
            )
            return

        try:
            overlaps = source_min <= db_max
        except TypeError as exc:
            raise ValueError(
                f"{self.config.name} overlap_check cannot compare source "
                f"{check.source_column!r} ({source_min!r}) with baseline "
                f"{check.db_column!r} ({db_max!r}): {exc}. Convert the source column to a "
                f"matching type (e.g. datetime/int) in field_config."
            ) from exc

        if overlaps:
            logger.info(
                "Overlap check passed for %s: source MIN(%s)=%s <= baseline %s MAX(%s)=%s.",
                destination.name,
                check.source_column,
                source_min,
                check.baseline,
                check.db_column,
                db_max,
            )
            return

        self._handle_overlap_failure(
            check,
            f"Overlap check FAILED for {destination.name}: source MIN({check.source_column})="
            f"{source_min} is later than baseline {check.baseline} MAX({check.db_column})="
            f"{db_max} on {baseline_table.display_name()}; loading would leave a gap "
            f"({db_max}, {source_min}) in the data.",
            logger,
        )

    @staticmethod
    def _handle_overlap_failure(check: OverlapCheck, message: str, logger: logging.Logger) -> None:
        if check.mode == "enforce":
            raise ValueError(message)
        logger.warning(message)

    def _align_for_destination(
        self,
        cnxn: Any,
        df: pd.DataFrame,
        table: TableRef,
        logger: logging.Logger,
    ) -> tuple[pd.DataFrame, list[str]]:
        if self.config.column_mapping:
            output_df = self._apply_column_mapping(df)
            insert_columns = list(output_df.columns)
            if self.config.use_db_column_order:
                db_columns = get_insert_columns(
                    cnxn,
                    table,
                    skip_identity_columns=self.config.skip_identity_columns,
                )
                mapped = set(insert_columns)
                missing_from_db = [column for column in insert_columns if column not in db_columns]
                if missing_from_db:
                    raise ValueError(f"Mapped columns do not exist in destination table: {missing_from_db}")
                insert_columns = [column for column in db_columns if column in mapped]
                output_df = output_df[insert_columns]
            return output_df, insert_columns

        if self.config.use_db_column_order:
            insert_columns = get_insert_columns(
                cnxn,
                table,
                skip_identity_columns=self.config.skip_identity_columns,
            )
            missing = [column for column in insert_columns if column not in df.columns]
            if missing and not self.config.allow_missing_destination_columns:
                raise ValueError(f"{self.config.name} missing destination columns: {missing}")
            output_df = df.copy()
            for column in missing:
                output_df[column] = None
            ignored = [column for column in output_df.columns if column not in insert_columns]
            if ignored:
                logger.info("Ignoring source columns not present in destination: %s", ignored)
            return output_df[insert_columns], insert_columns

        insert_columns = list(df.columns)
        return df, insert_columns

    def _apply_column_mapping(
        self,
        df: pd.DataFrame,
        mapping: list[dict[str, Any]] | None = None,
    ) -> pd.DataFrame:
        mapping = self.config.column_mapping if mapping is None else mapping
        output = pd.DataFrame(index=df.index)
        for item in mapping or []:
            destination = item.get("destination") or item.get("dest")
            if not destination:
                continue
            if item.get("type") == "ignore":
                continue
            if "value" in item:
                output[destination] = item["value"]
                continue
            source = item.get("source")
            if source is None:
                output[destination] = None
            elif source not in df.columns:
                raise ValueError(f"Mapping source column {source!r} is not in dataframe")
            else:
                output[destination] = df[source]
        return output

    def _log_health(
        self,
        *,
        process_id: str,
        source_paths: list[str],
        started_at: datetime,
        status: str,
        row_count: int | None,
        log_file_path: str,
        error: str | None,
        duration: int,
        destination_results: list[DestinationLoadResult],
        source_warning: str | None = None,
        log_note: str | None = None,
        logger: logging.Logger,
    ) -> None:
        health_table: TableRef | None = self.config.health_table
        if health_table is None:
            logger.warning("No health_table configured; skipping ETLHealth insert.")
            return

        step_rows = self._build_health_steps(
            destination_results=destination_results,
            status=status,
            row_count=row_count,
            error=error,
            duration=duration,
        )
        try:
            cnxn = connect_sql_server(health_table.server, health_table.database)
            try:
                for step in step_rows:
                    is_prod = step.step == "prod"
                    payload = {
                        "ProcessName": self.config.process_name,
                        "ProcessID": process_id,
                        "SourceFilePath": ";".join(source_paths) if source_paths else None,
                        "LastRunTime": started_at,
                        "TargetTableName": step.target.qualified_name(include_database=True),
                        "TaskStatus": step.status,
                        "RowCount": step.row_count,
                        "PackagePath": self.config.package_path,
                        "LogFilePath": log_file_path,
                        # The staging table(s) this row's target was loaded from. When
                        # the row's target *is* the staging table (a staging row), there
                        # is no separate staging source -> "Not Applicable". A prod row
                        # lists the primary staging table plus any aux staging tables
                        # (extra_staging_tables) that also feed the promotion.
                        "STGTableName": _stg_table_name(step),
                        "ProcessFrequency": self.config.process_frequency,
                        # Failed steps get a classified error; a successful step
                        # carries any non-fatal warnings (log fallback + source column).
                        "Error": self._health_error(step, source_warning, log_note),
                        "Duration": step.duration if step.duration is not None else duration,
                        # Server the step actually writes to (staging vs prod may differ).
                        "DBConnection": step.target.server,
                        # 'STG' or 'PRD' depending on which table this row is for. A
                        # direct-load destination (no prod block) types its single
                        # load step 'PRD': the loaded table IS the final table.
                        "TargetTableType": step.table_type,
                        # The load strategy for that step (a direct load runs the
                        # staging strategy even though its row is typed PRD).
                        "ProcessType": (
                            self.config.prd_load_strategy if is_prod else self.config.stg_load_strategy
                        ),
                    }
                    insert_health_record(cnxn, health_table, payload)
            finally:
                cnxn.close()
        except Exception:  # noqa: BLE001 - health logging should not hide the original loader result.
            logger.exception("Failed to write ETLHealth record")

    @staticmethod
    def _health_error(step: StepResult, source_warning: str | None, log_note: str | None) -> str | None:
        """Value for the ETLHealth ``Error`` column.

        Failed steps report a classified error. Successful steps report any
        non-fatal warnings: the log-fallback note (run-level, on every row) plus
        the source-column warning (on the staging row).
        """
        if step.status != "SUCCESS":
            return classify_error(step.error)
        notes = [note for note in (log_note, source_warning if step.step == "staging" else None) if note]
        return "; ".join(notes) or None

    def _build_health_steps(
        self,
        *,
        destination_results: list[DestinationLoadResult],
        status: str,
        row_count: int | None,
        error: str | None,
        duration: int,
    ) -> list[StepResult]:
        """One ETLHealth row per executed step: a staging-load row per destination,
        plus a prod-update row when prod promotion is configured."""
        if destination_results:
            step_rows: list[StepResult] = []
            for result in destination_results:
                if result.steps:
                    step_rows.extend(result.steps)
                elif result.status != "SUCCESS":
                    # A failed destination that produced no step rows still logs one;
                    # a successful destination with no steps (e.g. --prd-only on a
                    # destination without prod promotion) did no work, so log nothing.
                    step_rows.append(
                        StepResult(
                            step="staging",
                            target=result.destination.staging,
                            staging_table=result.destination.staging,
                            status=result.status,
                            row_count=result.row_count,
                            error=result.error,
                            duration=duration,
                            table_type="PRD" if result.destination.is_direct else "STG",
                        )
                    )
            return step_rows

        # The run failed before any destination loaded (e.g. read/transform error):
        # record one FAILED staging step per enabled destination.
        return [
            StepResult(
                step="staging",
                target=destination.staging,
                staging_table=destination.staging,
                status=status,
                row_count=row_count,
                error=error,
                duration=duration,
                table_type="PRD" if destination.is_direct else "STG",
            )
            for destination in self.config.destinations
            if destination.enabled
        ]

    def _build_logger(self, process_id: str) -> tuple[logging.Logger, Path, str | None]:
        stamp = datetime.now().strftime("%Y%m%d")
        log_dir, fallback_note = self._resolve_log_dir(stamp)
        log_file_path = log_dir / f"{self.config.name}_{process_id}.log"

        logger = logging.getLogger(f"infor_loader.{self.config.name}.{process_id}")
        logger.setLevel(self._log_level())
        logger.propagate = False
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")

        file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        if self.config.log_to_console:
            # Bind to the real stderr *now*, before _capture_streams replaces it,
            # so console output never feeds back into the capture writer.
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        if fallback_note:
            # Record why the configured log_root was not used, now that the logger exists.
            logger.warning(fallback_note)

        return logger, log_file_path, fallback_note

    def _resolve_log_dir(self, stamp: str) -> tuple[Path, str | None]:
        """Return the dated log directory to use, plus a warning note if the
        configured ``log_root`` was unusable and a local fallback was chosen.

        The logger is built before the run's try/except, so an unwritable
        ``log_root`` (e.g. an unreachable network share) must not crash the job:
        we fall back to a local ``logs\\<loader>`` folder and continue, so the
        load still runs and still writes its ETLHealth row.
        """
        primary = self.log_root / stamp
        try:
            primary.mkdir(parents=True, exist_ok=True)
            return primary, None
        except OSError as exc:
            fallback = Path("logs") / self.config.name / stamp
            fallback.mkdir(parents=True, exist_ok=True)
            note = (
                f"Configured log_root {self.log_root} is not usable ({exc.strerror or exc}); "
                f"logging to local fallback {fallback.resolve()} instead."
            )
            # The file logger does not exist yet, so surface this on stderr too.
            print(f"WARNING: {note}", file=sys.stderr)
            return fallback, note

    def _log_level(self) -> int:
        level = logging.getLevelName(str(self.config.log_level).upper())
        return level if isinstance(level, int) else logging.INFO

    @staticmethod
    def _close_logger(logger: logging.Logger) -> None:
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)


def _stg_table_name(step: StepResult) -> str:
    """ETLHealth STGTableName for a step. 'Not Applicable' when the row's target IS
    its staging table (a staging-load row has no separate staging source); otherwise
    the primary staging table plus any auxiliary staging tables that also fed the
    promotion (extra_staging_tables), semicolon-joined -- so a PRD row shows every
    staging table it was promoted from."""
    if step.target == step.staging_table and not step.extra_staging_tables:
        return "Not Applicable"
    tables = [step.staging_table, *step.extra_staging_tables]
    return "; ".join(table.qualified_name(include_database=True) for table in tables)


def _committed_row_count(cnxn: Any, table: TableRef) -> int | None:
    """Rows durably committed to ``table``, or None when the connection can no
    longer answer (e.g. it died mid-insert). Rolls back first so the count does
    not include the failed batch's uncommitted rows."""
    try:
        cnxn.rollback()
        return count_rows(cnxn, table)
    except Exception:  # noqa: BLE001 - diagnostic only; the load error is what gets raised.
        return None


def classify_error(error_text: str | None) -> str | None:
    """Bucket a failure into a short code for the ETLHealth ``Error`` column.

    The full traceback always lands in the per-run log file (see ``LogFilePath``);
    this column just records which well-known failure it was. Anything that does
    not match a known pattern is logged as ``See Log`` so the operator opens the
    log for detail.
    """
    if not error_text:
        return None
    text = error_text.lower()
    # A prod-promotion step marked failed solely because its staging load failed in the
    # same run (the promotion was never attempted). Matched exactly against the marker
    # set in _load_destination so a real traceback containing these words can't trip it.
    if text.strip() == "staging load failed":
        return "STAGING LOAD FAILED"
    if "filenotfounderror" in text or "no files matched" in text:
        return "FILE NOT FOUND"
    # An expected source column (that is actually loaded) is absent from the file.
    if "missing expected column" in text:
        return "COLUMN NOT FOUND"
    # SQL Server 208 ("Invalid object name ...") on INSERT/SELECT and 4701 ("Cannot
    # find the object ... because it does not exist ...") on TRUNCATE both mean the
    # destination (staging/prod) table is missing.
    if "invalid object name" in text or "cannot find the object" in text:
        return "TABLE NOT FOUND"
    # SQL Server 2627 ("Violation of PRIMARY KEY constraint ...") and the app-side
    # pk_check both mean a duplicate primary key.
    if "primary key" in text or "pk_check" in text:
        return "PK VIOLATION"
    # SQL Server 2627 UNIQUE constraint / 2601 unique index.
    if "unique key" in text or "unique index" in text:
        return "UX VIOLATION"
    # SQL Server 8152/2628: "String or binary data would be truncated ...".
    if "would be truncated" in text:
        return "FIELD TRUNCATE"
    # The staging insert stopped mid-load or landed fewer rows than the prepared
    # source (see _load_staging's guardrail). Checked after the specific SQL
    # causes above so e.g. a PK violation mid-load still classifies as the cause;
    # this catches interruptions with no better-known signature.
    if "partial staging load" in text:
        return "PARTIAL STAGING LOAD"
    return "See Log"


def _truthy_indicator(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


class _StreamToLogger:
    """File-like object that routes captured stdout/stderr writes into a logger.

    Lets stray ``print`` output, pandas/library warnings, and tracebacks that
    underlying code sends to the system streams land in the per-run log file
    (and console) instead of escaping the loader's logging.
    """

    def __init__(self, logger: logging.Logger, level: int) -> None:
        self._logger = logger
        self._level = level
        self._buffer = ""

    def write(self, message: str) -> int:
        if not isinstance(message, str):
            message = str(message)
        self._buffer += message
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.strip():
                self._logger.log(self._level, line)
        return len(message)

    def flush(self) -> None:
        if self._buffer.strip():
            self._logger.log(self._level, self._buffer.rstrip())
        self._buffer = ""

    def isatty(self) -> bool:
        return False


@contextmanager
def _capture_streams(logger: logging.Logger):
    """Redirect process stdout/stderr into ``logger`` for the wrapped block.

    stdout is logged at INFO and stderr at WARNING, so any error output a daily
    run prints to the system streams is preserved in the log file. Streams are
    always restored, even on failure.
    """
    stdout_writer = _StreamToLogger(logger, logging.INFO)
    stderr_writer = _StreamToLogger(logger, logging.WARNING)
    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = stdout_writer, stderr_writer
    try:
        yield
    finally:
        try:
            stdout_writer.flush()
            stderr_writer.flush()
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr
