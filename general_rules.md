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
- Verify a destination table before enabling it -- the CLI's read-only
  `table-info --server ... --database ... --schema ... --table ...` prints the
  live columns/nullability -- and note the verification date in the config.
- Master data with no staging table uses a direct-load destination (`table:`
  only) and writes its load settings under `direct_load:` -- an alias of
  `stg_load` (same `strategy` / `batch_size` fields) named for what actually
  happens: the final table is truncate+inserted in place. The CLI rejects
  `direct_load` on a loader with a staging/prod split, and rejects mixing it
  with `stg_load`. Transactional loaders use `staging:` / `prod:` with
  post_sql promotion and keep `stg_load` / `prd_load`.

## Other conventions

- `pk_check` lists destination column names; use `fail_on_pk_duplicates: false`
  only with a comment explaining why duplicates are tolerated.
- Keep `archive` enabled so each run records the exact inputs it consumed
  (including mapping side-files).
- Every load step writes an ETLHealth row -- keep the shared `health_table`
  block pointed at `SCSFileIngestor.InforLoader.ETLHealth`.
- Running a loader from this workstation truncates and loads LIVE prod tables;
  there is no dry run. Validate configs with `list`/`table-info` first.
