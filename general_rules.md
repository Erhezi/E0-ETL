# General rules for building loaders

Conventions every loader (YAML config + transform) should follow. The pipeline
enforces most of them centrally -- a new loader mostly needs to *not* fight them.

## Null handling

One rule per destination type, enforced centrally in
`infor_loader/transforms.py` (`apply_type_changes`, `normalize_for_db`) and
`infor_loader/db.py` (`_clean_value`):

| Destination type      | Blank/null in the source lands as | Enforced by |
| --------------------- | --------------------------------- | ----------- |
| date / datetime       | `1900-01-01` sentinel             | `apply_type_changes` fills blank/unparseable values |
| numeric (float / int) | `NULL` -- never force `0` / `0.0` | `_to_float` / `_to_int` keep NaN/None; `_clean_value` sends NULL |
| string                | `''` (empty string, trimmed*)     | `normalize_for_db` fills blanks and strips whitespace (*see whitespace exception below) |

Rules that follow from this for transform code:

- Never blanket-`df.fillna("")` in a transform: it stuffs `''` into numeric
  columns, which the destination's numeric column rejects at insert time. Use
  `_fill_string_blanks(df)` from `transforms.py` (fills object-dtype columns
  only) -- or nothing at all, since `normalize_for_db` fills string blanks
  after the transforms anyway.
- Never map numeric blanks to `0.0`/`0` "to match the old manual load" -- keep
  them null. (The vendor_item 0.0 forcing was retired on 2026-07-21.)
- `1900-01-01` is the single blank-date sentinel (prod was backfilled from
  `0001-01-01` on 2026-07-15). Don't invent per-loader sentinels.
- If a destination numeric column is NOT NULL, don't paper over it with a
  default in the transform: either the export must always ship the value, or
  the fill is a deliberate, commented decision in that loader.

### Whitespace exception: `preserve_whitespace_columns`

Strings are whitespace-trimmed by default. If a destination's primary key
relies on significant LEADING whitespace to keep near-duplicate rows distinct,
trimming collapses them into duplicate keys and the insert dies mid-load,
after the truncate. SQL Server semantics to remember: varchar key comparisons
ignore TRAILING spaces but treat a leading space as a distinct value.

For such loaders, list the affected source columns in the top-level
`preserve_whitespace_columns` config (a sibling of `fail_on_pk_duplicates`);
they are still `''`-filled when blank, just not trimmed. Real example:
mdm_vendor_item -- the Infor export ships 19 vendor items twice (clean +
leading-space variant) and both destination tables' PK_VendorItem accepts the
pair only untrimmed. Don't hack around it in a transform.

## Typing / mapping

- Type every loaded column in `field_config.mapping`. `apply_type_changes` runs
  BEFORE transforms, so a transform already sees floats comma-stripped
  (`"1,000.0000000"` -> `1000.0`) and dates parsed + sentinel-filled -- don't
  re-convert them in the transform.
- Set `datetime_format` (and `date_format` when the export is unambiguous)
  explicitly so pandas parses fast instead of guessing per row.
- A source column carrying a time-of-day that lands in a DATE column must be
  typed `datetime` in the mapping; the loader truncates it client-side (a
  strict `date` parse would coerce every value to the 1900-01-01 sentinel).

## Scheduling / destinations

- Daily transactional loaders: tag `daily`, `process.frequency: Workdays`.
- Loaders land BOTH destinations unless there is a documented reason not to:
  - `des1`: `MISCPrdAdhocDB` / `PRIME` / `DM_MONTYNT\dli2`
  - `des2`: `YNBBSTVWP02\PROCDATASRVPROD` / `PLMPreprocessorShared` / `infor`
  Name them `des1`/`des2` consistently: the run-time `--destination <name>` flag
  (one-off single-side run) and `data_fill_helper --from/--to` both address a
  destination by name across loaders. To pause a side for good, set
  `enabled: false` on its block -- `--destination` is for one run, not a config
  change, and never re-enables a disabled destination.
- Verify a destination table before enabling it -- the CLI's read-only
  `table-info --server ... --database ... --schema ... --table ...` prints the
  live columns/nullability -- and note the verification date in the config.
- Declare EVERY prod table a promotion writes: the main one as `prod.table`, any
  others as `prod.aux` (`name: table`). The `post_sql` procs do the writing
  either way, but an undeclared table gets no ETLHealth row and cannot be moved
  between destinations by `data_fill_helper --table <name>`. Use the same `aux`
  name on every destination for the same logical table (`stat` =
  `InforContractLineErrorStat` on des1, `ContractLineErrorStat` on des2) --
  mismatched names are rejected at config load.
- Master data with no staging table uses a direct-load destination (`table:`
  only) and writes its load settings under `direct_load:` -- an alias of
  `stg_load` (same `strategy` / `batch_size` fields) named for what actually
  happens: the final table is truncate+inserted in place. The CLI rejects
  `direct_load` on a loader with a staging/prod split, and rejects mixing it
  with `stg_load`. Transactional loaders use `staging:` / `prod:` with
  post_sql promotion and keep `stg_load` / `prd_load`.

## Post-load processes

Jobs that run *after* the loaders (PLM, Preprocessor, BullardBurnDown) are
declared in `configs\post_processes\<name>.yaml`, not as loaders — they read no
file and have no staging/prod split.

- Declare what a process needs under `requires.loaders` using loader **names**
  (`inventory_location`), never the friendly ProcessName. Keep
  `scope: destination` so one side's failure can't block the other, and
  `on_unmet: block` so an unmet gate records BLOCKED instead of running on
  stale data.
- One ETLHealth row per process per destination, typed `PROC` — the verdict
  only. Sub-procedure detail belongs in that process's own `process_log` table
  and in the run's log file; don't fan it out into ETLHealth rows.
- Keep steps ordered and idempotent: the first failure stops the destination,
  and the fix is re-running the whole process, not resuming mid-way.
- Put Python steps in `infor_loader\processes\`, one callable per step taking a
  `StepContext`. Raise on failure — the runner records it and logs the
  traceback; never swallow an error to keep a batch green.
- Mirror the loaders' destination names (`des1`/`des2`) so `--destination`
  addresses the same side across loaders and processes.

## Other conventions

- `pk_check` lists destination column names; use `fail_on_pk_duplicates: false`
  only with a comment explaining why duplicates are tolerated.
- Keep `archive` enabled so each run records the exact inputs it consumed
  (including mapping side-files).
- Every load step writes an ETLHealth row -- keep the shared `health_table`
  block pointed at `SCSFileIngestor.InforLoader.ETLHealth`.
- Running a loader from this workstation truncates and loads LIVE prod tables;
  there is no dry run. Validate configs with `list`/`table-info` first.
