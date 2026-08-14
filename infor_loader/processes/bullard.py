"""BullardBurnDown daily steps, ported from ``BullardBurnDownDaily.ipynb``.

Two steps, run in order against des1:

1. :func:`insert_daily_archive` -- ``sp_InsertDailyArchive`` once per date across a
   short trailing window, so late-arriving data is picked up on the following runs.
2. :func:`build_search_terms` -- read ``vw_SearchHelperPreAgg``, aggregate the search
   terms per item / item group in pandas, and truncate+reload ``SearchTerms``.

The aggregation is kept exactly as the notebook computed it (``'|'.join(x.unique())``
over each group, item-group terms falling back to item terms) so the table this
produces is byte-for-byte what the manual run produced. What is new is the loading:
the notebook's positional executemany is replaced with the framework's
:func:`~infor_loader.db.insert_dataframe`, which cleans NaN/NaT to NULL, commits in
batches and lets the caller verify the landed row count.
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

import pandas as pd

from ..db import count_rows, get_insert_columns, insert_dataframe, truncate_table

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to type checkers
    from ..post_process import StepContext


# Columns the notebook's aggregation reads out of vw_SearchHelperPreAgg.
_REQUIRED_VIEW_COLUMNS = ["Item Group", "Item", "RefSearch", "ItemSearch", "Date"]

# Columns written to SearchTerms, in the order the destination table declares them.
_OUTPUT_COLUMNS = ["Item", "RefsSearch", "ItemsSearch", "Date"]


def insert_daily_archive(ctx: "StepContext") -> None:
    """Run ``sp_InsertDailyArchive`` for each date in the trailing window.

    ``backfill_days: 3`` (the notebook's default) archives FOUR dates -- today and
    the three before it -- because the source data for a given day keeps settling
    after that day closes. The proc is idempotent per date, which is what makes the
    same window safe to re-run every morning.

    Each date is committed on its own, unlike the notebook's single commit at the
    end: a failure partway through then leaves the earlier dates archived instead of
    discarding the whole window, and the re-run redoes only what is left.
    """
    backfill_days = int(ctx.options.get("backfill_days", 3))
    if backfill_days < 0:
        raise ValueError(f"backfill_days must be >= 0; got {backfill_days}.")
    procedure = str(ctx.options.get("procedure", "BullardBurnDown.sp_InsertDailyArchive"))

    end_date = ctx.report_date
    start_date = end_date - dt.timedelta(days=backfill_days)
    ctx.logger.info(
        "    archiving %s date(s): %s .. %s",
        backfill_days + 1,
        start_date.isoformat(),
        end_date.isoformat(),
    )

    cursor = ctx.cnxn.cursor()
    try:
        for offset in range(backfill_days + 1):
            archive_date = start_date + dt.timedelta(days=offset)
            cursor.execute(f"EXEC {procedure} @ArchiveDate = ?", archive_date)
            _drain_results(cursor)
            ctx.cnxn.commit()
            ctx.logger.info("    archived %s", archive_date.isoformat())
    finally:
        cursor.close()


def build_search_terms(ctx: "StepContext") -> int:
    """Rebuild the ``SearchTerms`` fixture from ``vw_SearchHelperPreAgg``.

    Returns the number of rows loaded.
    """
    source_view = str(ctx.options.get("source_view", "BullardBurnDown.vw_SearchHelperPreAgg"))
    target_table = str(ctx.options.get("target_table", "SearchTerms"))
    batch_size = int(ctx.options.get("batch_size", 50_000))

    engine = ctx.engine()
    try:
        search_helper = pd.read_sql_query(f"SELECT * FROM {source_view}", engine)
    finally:
        engine.dispose()
    ctx.logger.info("    read %s row(s) from %s", len(search_helper), source_view)

    missing = [column for column in _REQUIRED_VIEW_COLUMNS if column not in search_helper.columns]
    if missing:
        raise ValueError(
            f"{source_view} is missing expected column(s) {missing}; "
            f"it returned {list(search_helper.columns)}."
        )

    # Aggregation exactly as the notebook computed it. Note `.unique()` keeps NaN,
    # so a null RefSearch/ItemSearch raises here rather than silently producing a
    # different fixture than the manual run did -- the view is expected to have none.
    refs_by_group = search_helper.groupby("Item Group")["RefSearch"].transform(_join_unique)
    refs_by_item = search_helper.groupby("Item")["RefSearch"].transform(_join_unique)
    # Rows with no Item Group fall out of the first groupby as NaN and take the
    # item-level terms instead.
    search_helper["RefsSearch"] = refs_by_group.fillna(refs_by_item)
    search_helper["ItemsSearch"] = search_helper.groupby("Item")["ItemSearch"].transform(_join_unique)

    output = search_helper[_OUTPUT_COLUMNS].copy().drop_duplicates()
    ctx.logger.info("    aggregated to %s distinct search-term row(s)", len(output))

    table = ctx.table(target_table)
    db_columns = get_insert_columns(ctx.cnxn, table, skip_identity_columns=True)
    if len(db_columns) != len(output.columns):
        raise ValueError(
            f"{table.display_name(include_server=True)} has {len(db_columns)} insertable "
            f"column(s) {db_columns} but the prepared frame has {len(output.columns)} "
            f"{list(output.columns)}."
        )
    if [column.lower() for column in db_columns] != [column.lower() for column in output.columns]:
        # The notebook inserted positionally against the destination's column order;
        # keep that contract, but say so when the names do not line up.
        ctx.logger.warning(
            "    SearchTerms column names differ from the prepared frame; inserting "
            "positionally: %s -> %s",
            list(output.columns),
            db_columns,
        )
    output.columns = db_columns

    expected_rows = len(output)
    truncate_table(ctx.cnxn, table)
    row_count = insert_dataframe(ctx.cnxn, table, output, db_columns, batch_size=batch_size)
    landed_rows = count_rows(ctx.cnxn, table)
    if landed_rows != expected_rows:
        raise RuntimeError(
            f"PARTIAL LOAD: {table.display_name(include_server=True)} holds {landed_rows} "
            f"row(s) after the load but the prepared frame has {expected_rows}; the table "
            f"was truncated at the start of this step, so it is incomplete until the next "
            f"successful run."
        )
    ctx.logger.info("    loaded %s row(s) into %s", row_count, table.display_name())
    return row_count


def _join_unique(values: pd.Series) -> str:
    """``'|'``-joined distinct values of one group, as the notebook built them."""
    return "|".join(values.unique())


def _drain_results(cursor: Any) -> None:
    """Consume any result sets a procedure returned so the connection can commit.

    ``sp_InsertDailyArchive`` returns none today; a SELECT added inside it later
    would otherwise leave the connection busy and fail the commit mid-window.
    """
    try:
        while cursor.nextset():
            pass
    except Exception:  # noqa: BLE001 - nothing left to drain is the normal case.
        pass
