# Infor File Loaders

The loader package is driven by one YAML file per data source under
`configs\loaders\`. The `configs\` root holds the shared
`file_folder_loader_config.yaml` registry (see "Input files" below); the loaders
are grouped into subfolders by how they run:

```text
configs\
  file_folder_loader_config.yaml   # input-file registry (not a loader)
  loaders\
    daily\                         # run unattended as the daily batch (--all)
      inventory_location.yaml      # one YAML per data source
      ...
    on-demand\                     # run by hand only (never in --all)
      ghx_completed_invoice_by_date.yaml
    backfill\                      # NOT loaders: one-time column backfills driven
      payablesinvoice_header.yaml  # by backfill_helper (see "Column Backfill")
  post_processes\                  # run AFTER the loaders (see "Post-load processes")
    plm.yaml
    preprocessor.yaml
    bullard_burn_down.yaml
    payablesinvoice_vendor_gl_index.yaml
```

Commands still take `--config configs` (the default): the loaders are discovered
**recursively** under `configs\loaders\` (so `daily\`, `on-demand\` and
`backfill\` are all found), and the registry is found at the `configs\` root.
The grouping is by `loaders\<group>\`; what actually gates a loader out of the
daily batch is the `on-demand` **tag** (see "On-demand loaders" below), not the
folder name. `post_processes\` is a **sibling** of `loaders\`, not under it, so
its YAMLs are never swept into `--all`.

Everything under `loaders\` must **parse** as a `LoaderConfig` — a malformed YAML
there breaks discovery for every command. That is why the `backfill\` configs are
loader-shaped even though they are not loaders: they carry `enabled: false` (so
nothing in the loader CLI will run one) and an extra `backfill:` block that
`LoaderConfig` ignores and `backfill_helper` reads.

### On-demand loaders

A loader tagged `on-demand` is **excluded** from the daily `--all` batch and from
the `move-files --check` source scan, but is still discovered so it can be run
explicitly by name:

```powershell
python -B run_daily_loaders.py --loader ghx_completed_invoice_by_date
```

Keep on-demand loaders under `configs\loaders\on-demand\` and give them the
`on-demand` tag (and **not** the `daily` tag). Tag their registry input
`on-demand` too: an on-demand input still lands in Downloads and has a normal
`download` block, but the tag keeps it out of the **unfiltered** daily
`move-files` dispatch (and the daily `--check` scan), so it is not pulled from
Downloads before its loader runs. The loader's own run-time download gate stages
it when you run the loader; you can also relocate it by hand with
`move-files --input <key>` (or `move-files --tag on-demand`).

## Run Commands

List configured YAML loaders:

```powershell
python -B run_daily_loaders.py list
```

Run Inventory Location. The bare `--loader <name>` form prints a summary
(enabled/tags, resolved source files, staging/prod destinations, load
strategies) and asks for `yes`/`no` before touching any table:

```powershell
python -B run_daily_loaders.py --loader inventory_location
```

Add `--auto` to skip the confirmation prompt and run immediately (use this for
unattended/Task Scheduler runs):

```powershell
python -B run_daily_loaders.py --loader inventory_location --auto
```

Run several loaders at once. The selector flags — `--loader`, `--tag`, and
`move-files --input` — each take **multiple names**: space-separate them, or
repeat the flag; both work and can be mixed.

```powershell
# space-separated
python -B run_daily_loaders.py --loader inventory_location item supplier --auto --max-workers 4
# repeated flag (equivalent)
python -B run_daily_loaders.py --loader inventory_location --loader item --loader supplier --auto --max-workers 4
# by tag (e.g. the whole master-data batch)
python -B run_daily_loaders.py --tag mdm --auto
```

For Windows Task Scheduler, set the working directory to this folder and use the
`--auto` form above (or the equivalent `python -B run_daily_loaders.py run ...`
subcommand, which never prompts — it also accepts `--auto` as a harmless no-op,
so the same command line works with or without `run`).

### Running staging and prod separately

The file→staging load and the staging→prod promotion are independent: staging is
committed on its own connection before prod runs, and a prod failure never rolls
back staging. So you can split the two phases (mutually exclusive; work with both
the `--loader` form and the `run` subcommand):

```powershell
# Load the file into staging only; leave prod untouched (inspect staging first).
python -B run_daily_loaders.py --loader inventory_location --stg-only

# Later, promote the already-loaded staging table into prod without re-reading
# the file (e.g. after a prod-side EXEC failed on the previous full run).
python -B run_daily_loaders.py --loader inventory_location --prd-only
```

`--prd-only` reads no source file and skips the staging load — it only runs each
destination's `prod.post_sql`. `--stg-only` does not run `post_file_moves`, so the
source file stays in place for a later `--prd-only` or a re-run.

### Running one destination

A loader normally lands **both** destinations (`des1` and `des2`) on every run.
`--destination <name>` narrows a run to one side without editing config — useful
when one server was down, or when only that side's promotion needs a re-run:

```powershell
# Load only the PRIME (des1) side of Contract Line Error.
python -B run_daily_loaders.py --loader contract_line_error --destination des1

# Re-promote just des2 after its prod EXEC failed, off the staging data already there.
python -B run_daily_loaders.py --loader contract_line_error --destination des2 --prd-only

# Works with any selector, and takes several names (space-separated or repeated).
python -B run_daily_loaders.py --tag mdm --destination des2 --auto
```

The skipped destinations are skipped **entirely** — no staging load, no prod
promotion, no ETLHealth rows — exactly as if they were `enabled: false`. The flag
only ever narrows: a destination already disabled in YAML stays skipped even when
named. The summary lists only the destinations that will actually run, so what you
confirm is what gets loaded.

A selected loader that declares none of the named destinations (or has them all
disabled) drops out of the run with a note on stderr, since it would load nothing;
if no selected loader declares the name at all, the run exits `2` rather than
quietly loading everything — the usual cause is a typo. Both the `--loader` form
and the `run` subcommand accept the flag.

## Input files: the central registry + `move-files`

`configs\file_folder_loader_config.yaml` is the **single source of truth** for
where each input file lives and how it arrives. Two consumers resolve from it,
so they cannot drift:

- **The loaders** — each `source.files` entry references an input by key
  (`input: poline`) instead of restating its `path`/`name`. The loader keeps
  only its read-time concerns (`alias`, `reader`, `options`).
- **`move-files`** — relocates each downloaded export to its input's folder +
  canonical name, **before** the loaders run. Filesystem only: no database, no
  confirmation prompt, and `--dry-run` previews everything.

The registry has two blocks:

```yaml
folders:                         # named locations — THE batch-change knob.
  downloads:   'C:\Users\dli2\Downloads'
  temp_export: 'C:\...\INFOR_SC\temp export'
  misc_mdm:    'C:\...\INFOR_SC\misc mdm'

inputs:                          # one entry per physical input file
  item:
    folder: misc_mdm             # where the loader reads it (a folders key)
    name:  'Item.csv'            # the canonical file name
    tags:  [daily, mdm]
    download:                    # omit for a fixture maintained in place
      patterns: ['Item.csv', 'Item (*).csv']
```

Relocating a folder is a **one-line edit** to `folders:` — every loader and
download rule that references it by name follows, with no per-loader change.
That is the point of the registry: change folders in batch, not one loader at a
time.

```powershell
python -B run_daily_loaders.py move-files --list        # show the download inputs
python -B run_daily_loaders.py move-files --dry-run      # preview, touches nothing
python -B run_daily_loaders.py move-files                # dispatch every download
python -B run_daily_loaders.py move-files --tag mdm      # just the mdm exports
python -B run_daily_loaders.py move-files --input item   # a single input by key
python -B run_daily_loaders.py move-files --check        # ...then verify every enabled
                                                         # loader's source files resolve
```

Semantics (see the registry's comments for the full story):

- `folders`: the only place absolute paths live. `download_defaults` supplies
  the fallback download source folder + move behavior every `download` block
  inherits.
- `inputs[].download.patterns`: exact download name plus the browser's
  `name (N).csv` re-download variant. Never a prefix glob like `Item*.csv`,
  which would also swallow `ItemGTIN.csv` / `ItemUOM.csv` / `ItemReplenishFrom.csv`.
  Whichever variant matches is renamed to the input's `name`.
- An input with **no `download` block** is a fixture (maintained in place, e.g.
  `company_map.csv`) — the loaders still read it, but `move-files` never
  dispatches it and selecting it explicitly is an error.
- `pick_latest` (default true): newest match by mtime wins. `action: move`
  (default): Downloads is a chute, not a store — the per-loader `archive` step
  keeps the consumed-input history. `required: false` (default): an input with
  nothing in Downloads only warns, since the file already in the designated
  folder is still valid. `on_exists: replace` (default): a fresh download
  replaces the file already there (`skip` / `fail` also available).
- `--check` resolves **every enabled loader's** source files and exits non-zero
  on a `MISSING` line — the real presence guard, since a missing download only
  warns.

The two groups in the registry (transactional `staging→prod`, and direct-load
`mdm`) are **organizational only** — a human troubleshooting aid (direct-load
problem → go to the raw file; staging→prod problem → check `_stg` first, then
the raw file). No behavior hangs on the grouping.

One input's failure never blocks the rest; the exit code is non-zero if any
input errored (or `--check` found a missing source). The registry lives at the
`configs\` root (one level above the loader YAMLs in `configs\loaders\`); the
loader loader finds it by looking in the configs directory and its parents, so
`--config configs` and a single `configs\loaders\<name>.yaml` both resolve it. A
loader may still use an explicit `path`/`name` instead of `input:` (the
reference is optional), but prefer the registry so the location stays defined in
one place.

## YAML Shape

Each YAML specifies:

- `connection`: default SQL Server and database.
- `source.files`: one or more input files, each independently configurable. Prefer `input: <key>` to reference the file's location from the central registry (see "Input files" above); `alias`/`reader`/`options` stay here. An explicit `path`/`name` still works.
- `destinations`: one or more SQL Server targets. Each target has a staging table loaded by the ETL and an optional prod table used as the target table in health logging/post-load workflows.
- `health_table`: `[InforLoader].[ETLHealth]` target.
- `logging.log_root`: where per-run log files go (a `YYYYMMDD` subfolder is created under it). The `<loader name>` placeholder is replaced with the loader's name, so a shared template like `\\host\share\DailyLoader\<loader name>\logs` works across loaders. If `log_root` can't be created/written (e.g. an unreachable share), the loader does **not** crash: it warns, falls back to a local `logs\<loader>\<date>` folder, still runs, and records the fallback note in the successful run's ETLHealth `Error` column so the degraded logging is visible.
- `logging.level`: log level (`INFO` by default).
- `logging.console`: also echo run logs to the console/stderr (default `true`), so Task Scheduler captures them.
- `logging.capture_streams`: redirect process `stdout`/`stderr` into the per-run log file (default `true`), so stray prints, library warnings, and uncaught tracebacks are logged when a run errors instead of escaping to the console only.
- `archive.path`: after a successful load, each consumed source file is **copied** here with a `_YYYYMMDD` stamp appended to the name (original extension kept), e.g. `Inventory_Location_20260707.csv`. Supports the `<loader name>` placeholder. Set `archive.enabled: false` (or omit the block) to disable. Archiving runs for full and `--stg-only` runs, is skipped for `--prd-only` (no file is read), and is best-effort: a copy failure is logged but does not fail the load. The original source file is left in place (it is copied, not moved).
- `field_config.mapping`: table-ordered list of the columns that get **loaded**. Each row is `[source_or_computed, destination, type, destination_sql_type, origin]`, where `origin` is `source` (a required column read from the file) or `computed` (produced by a transform/loader, so it is not in the file).
- `field_config.transform_inputs`: source columns a transform **consumes** but that are not loaded directly (e.g. an account field split into index/GL parts). Required — a missing one hard-fails just like a loaded column.
- `field_config.extra`: columns present in the file but **not integrated** into the pipeline yet. Optional — a missing one only warns.
- `field_config.pk_check`: duplicate-key validation columns (destination names).
- `field_config.datetime_format` / `field_config.date_format`: optional [strptime](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) formats for `datetime`/`date`-typed source columns, e.g. `'%m/%d/%Y %I:%M:%S %p'` for `07/20/2025 10:23:07 PM`. Set these to parse fast and unambiguously; when omitted, columns are parsed as `mixed` (per-value, no warning but slower).
- `destinations[].prod.post_sql`: statements (e.g. `EXEC` promotion procs) run on the **prod** connection after the staging load succeeds, to update prod from the just-loaded staging data.
- `destinations[].prod.aux`: `name: table` map of the **other** prod tables those statements write (see "Promotions that write several prod tables"). Same server/database/schema as `prod` unless overridden.
- `destinations[].post_sql`: optional statements run on the **staging** connection right after the staging load (staging-side cleanup).

Destination targets use this shape:

```yaml
destinations:
  - name: server_a
    server: SERVER_A
    database: DB_A
    schema: SCHEMA_A
    staging:
      table: inventory_location_stg
    prod:
      table: INVENTORY_LOCATION
  - name: server_b
    server: SERVER_B
    database: DB_B
    schema: SCHEMA_B
    staging:
      table: inventory_location_stg
    prod:
      table: INVENTORY_LOCATION
```

The destination-level `server`/`database`/`schema` are defaults that both `staging` and `prod` inherit. To land `staging` in a different database (or server or schema) than `prod`, set those keys inside the block:

```yaml
  - name: server_b
    server: SERVER_B
    database: PROD_DB        # default for both blocks
    schema: infor
    staging:
      database: STAGING_DB   # staging overrides only the database
      schema: stg
      table: item_location_stg
    prod:
      table: ITEM_LOCATION   # inherits SERVER_B.PROD_DB.infor
```

The compact string form (`staging: inventory_location_stg`) is still accepted and inherits all three from the destination. If `server` or `database` is omitted from a destination, the value is inherited from `connection`. The loader reads and transforms the source once, then loads each configured `staging` table on its own connection.

### Staging load, then prod promotion

Per destination the loader runs two steps:

1. **Staging load** — truncate + insert the transformed data into `staging` on the staging connection. Any `destinations[].post_sql` runs here (staging connection).
2. **Prod update** — if `prod.post_sql` is set, the loader opens the **prod** connection and runs those statements (typically `EXEC` of a promotion proc that updates prod from the just-loaded staging table). This only runs when the staging load for that destination succeeded.

```yaml
    prod:
      table: INVENTORY_LOCATION
      post_sql:
        - EXEC [infor].[sp_UpdateItemLocation]
```

Each step writes its own **ETLHealth** row sharing the run's `ProcessID`: a staging row (`TargetTableName` = the staging table) and, when prod promotion runs, a prod row (`TargetTableName` = the prod table). `TargetTableName` and `STGTableName` are bracket-qualified as `[db].[schema].[table]`; on `STG` rows (where the target *is* the staging table) `STGTableName` is logged as `Not Applicable`. Each row also records `DBConnection` (the server that step wrote to), `TargetTableType` (`STG` or `PRD`), and `ProcessType` (the load strategy for that table type — `stg_load.strategy` on `STG` rows, `prd_load.strategy` on `PRD` rows).

A failed step is logged `FAILED`; the full traceback goes to the per-run log file (`LogFilePath`), and the `Error` column records a short classification of common failures:

| `Error` value | Meaning |
| --- | --- |
| `FILE NOT FOUND` | the source file could not be located; a multi-input loader names the missing input(s), e.g. `FILE NOT FOUND: expected file(s) fd3, fd5 missing` |
| `COLUMN NOT FOUND` | an expected **loaded** source column is absent from the file |
| `PK VIOLATION` | primary-key duplicate (SQL insert or the app-side `pk_check`) |
| `UX VIOLATION` | unique key / unique index violation |
| `FIELD TRUNCATE` | a source value exceeded the destination column's length |
| `See Log` | anything else — open `LogFilePath` for detail |

A failed step stops that destination's remaining steps; other destinations still run.

### Promotions that write several prod tables

Some promotions update more than one production table — `contract_line_error` merges
the error rows and then writes a daily error-count snapshot. List the extra tables
under the destination's `prod.aux` (`name: table`), alongside the statements that
write them:

```yaml
    prod:
      table: InforContractLineError
      aux:
        stat: InforContractLineErrorStat
      post_sql:
        - EXEC [Preprocessor].[sp_UpdateContractLineErrors]
        - EXEC [Preprocessor].[sp_InsertInforContractLineErrorStat]
```

The `post_sql` statements still do all the work — `prod.aux` only declares what they
touch, which buys two things: each declared table gets its own `PRD` **ETLHealth** row
(same status/error/duration, since one statement batch writes them all; only the
primary table carries the `RowCount`), and `data_fill_helper` can copy any of them
between destinations by name. An `aux` name is the *same logical table* on every
destination — `stat` is `InforContractLineErrorStat` on des1 and
`ContractLineErrorStat` on des2 — so every enabled destination must declare the same
names, and `main` is reserved for the destination's own `prod.table`. (The daily
email consolidates rows per process/server/table type, so the extra rows show up in
ETLHealth itself, not as extra lines in the report.)

**Missing expected columns.** A source column is *required* when the pipeline needs it — either it is loaded to a destination (a `mapping` row with `origin: source`) or it is a `transform_inputs` entry. If a required column is missing, the run fails with `COLUMN NOT FOUND` before any table is touched. A column listed under `extra` is only present-but-unintegrated, so its absence cannot affect the output: the run **continues** and the successful staging row's `Error` column carries a non-fatal warning naming the missing columns, e.g. `COLUMN NOT FOUND (warning): expected file column(s) OrderMultiple missing; not loaded, ETL continued.`

(The legacy top-level `load.post_sql` still executes on the staging connection if present, but prefer `prod.post_sql` for promotion so it runs on the prod connection.)

Source columns fall into four roles, expressed by three `field_config` blocks:

| Role | Where it goes | Missing from file |
| --- | --- | --- |
| Required, cleansed + loaded | `mapping` row, `origin: source` | **fail** (`COLUMN NOT FOUND`) |
| Produced by a transform, then loaded | `mapping` row, `origin: computed` | n/a (not in the file) |
| Required input consumed by a transform, not loaded | `transform_inputs` | **fail** |
| Present but not integrated yet | `extra` | warn, continue |

`mapping` rows are ordered like the destination table and use this compact format:

```yaml
mapping:
  # [source_or_computed, destination, python_conversion_type, destination_sql_type, origin]
  - [InventoryLocation, Location, string, varchar(20), source]
  - [StockOnHandQuantity, OnHandQty, int, int, source]
  - [report stamp, report stamp, datetime, datetime, computed]   # produced by a transform
transform_inputs:      # required source columns a transform splits/consumes, not loaded directly
  - OffsetAccount
  - InventoryAccount
extra:                 # in the file but not integrated yet; optional
  - Item.ReplacementItem
```

`origin` is `source` when the field is read from the raw input file and `computed` when a transform/loader produces it. Types can be `string`, `date`, `datetime`, `float`, or `int`. Every `mapping` row needs all five fields; columns that are not loaded go under `transform_inputs` or `extra`, not in `mapping`.

## Utilities

Inspect destination columns:

```powershell
python -B run_daily_loaders.py table-info --server MISCPrdAdhocDB --database PRIME --schema "DM_MONTYNT\dli2" --table inventory_location_stg
```

Generate a draft mapping from DB metadata and source headers:

```powershell
python -B run_daily_loaders.py mapping-template --loader inventory_location --output mapping_templates\inventory_location.json
```

The mapping-template command reads the YAML, applies configured renames/drops, then compares the source headers to the destination table.

## Data Fill (`data_fill_helper`)

`data_fill_helper` pre-populates one destination's **prod** table from another
destination's **prod** table for the same loader config. Unlike the daily
loaders, its input is an existing prod table (not a file) — use it for one-time
back-fills, e.g. seeding a newly added destination's prod table from an existing
one.

It reads rows from the **source** destination's prod table and inserts them into
the **target** destination's prod table, copying only the target's insertable
(non-identity) columns that also exist in the source. By default the source is
the first destination in the YAML and the target is the second; override with
`--from`/`--to`.

This works for **direct-load** loaders too (e.g. the `ppe_*` reference tables):
a direct-load destination has no separate prod table — its single configured
table *is* the production table — so the tool uses that table (`prod or staging`)
on each side. `python -B data_fill_helper.py --loader ppe_category` copies the
des1 PPE table into the des2 PPE table.

**Loaders with several prod tables** (extra ones declared under `prod.aux`, see
above) copy one table per run: `--table <name>` picks it — `main` (the default) is
the destination's own `prod` table, or use the `aux` name — and `--all-tables`
copies every one in declaration order, primary table first, each with its own
confirmation. Declining one stops the run, leaving the remaining tables untouched.

**Write modes:**

- *Default (insert only if empty)* — the target prod table must have **zero
  rows**, otherwise the copy is skipped without writing.
- *`--truncate`* — **TRUNCATE** the target prod table first, then fill it
  regardless of its current row count. Use this to overwrite an already-populated
  target.

Every write to prod asks for an interactive `yes` confirmation first. `--yes`
skips the prompt; `--dry-run` reads and reports only and never writes (combine
with `--truncate` to preview how many rows would be removed and copied).

> ⚠️ This touches **live prod** tables. There is no undo — `--truncate`
> permanently removes the target's existing rows before copying.

Preview a copy (no writes):

```powershell
python -B data_fill_helper.py --loader inventory_location --dry-run
```

Copy into an empty target (prompts for confirmation):

```powershell
python -B data_fill_helper.py --loader inventory_location
```

Truncate the target first, then refill — choosing explicit source/target and
skipping the prompt:

```powershell
python -B data_fill_helper.py --loader inventory_location --from server_a --to server_b --truncate --yes
```

Seed both of a loader's prod tables on the other destination:

```powershell
python -B data_fill_helper.py --loader contract_line_error --all-tables --dry-run
python -B data_fill_helper.py --loader contract_line_error --all-tables
```

The bare config name also works in place of `--loader <name>`
(e.g. `python -B data_fill_helper.py --inventory_location`). On completion it
prints one status line per copied table, e.g.
`COPIED  inventory_location  main  rows_truncated=120  rows_copied=120`;
status is one of `COPIED`, `DRY_RUN`, `SKIPPED_NONEMPTY`, or `ABORTED`.

## Column Backfill (`backfill_helper`)

`backfill_helper` fills columns that were **added to an already-populated prod
table**. When a table grows columns (an `ALTER TABLE ... ADD`), every row already
in it keeps NULL there — the daily loader only refills a row when its key comes
back around in the rolling export window, which for closed history is never. The
gap is closed from a one-off export carrying the table's key plus the new
columns, handed over in as many chunks as the reporting tool will produce.

It is **not a loader**: it writes no ETLHealth row (a one-time backfill is not a
scheduled process and would show on the daily health report as an unexplained
extra job), and it reads no download gate.

### Two phases

1. **Stage** — the export chunk is truncate/inserted into a per-destination
   `*_backfill` table through the **ordinary loader pipeline**: rename, type
   conversion, source→destination mapping, PK duplicate check, varchar-width
   clamp, batched `fast_executemany` insert. The table is created on first use
   **from the target table's own column definitions**, so a staged value can
   never be widened, narrowed or rounded on its way into the column it will be
   written to.
2. **Merge** — a generated `MERGE ... WHEN MATCHED THEN UPDATE` copies the
   configured columns into the target, joined on the target's key.
   `WHEN NOT MATCHED` is deliberately absent: a backfill fills columns on rows
   the target already has, so a key the target does not carry is left alone
   rather than inserted as a row with most columns NULL.

Both phases run on one connection per destination, because a backfill config's
`staging` and `prod` blocks are the same server and database — which is what lets
a single MERGE see both.

The merge runs in `batch_rows`-sized slices of the backfill table's
`BackfillRowId` identity column, each its own transaction, so a half-million-row
chunk is not one lock-holding, log-growing statement against live prod.

### Config

Backfill configs live in `configs\loaders\backfill\` and are ordinary
`LoaderConfig` YAMLs with the two table roles reinterpreted — `staging:` is the
backfill table, `prod:` is the table being filled — plus a `backfill:` block the
loaders ignore. They carry `enabled: false` and the `backfill` + `on-demand`
tags, so they are parsed and listed but the loader CLI will never run one
(`--loader <name>` reports "No enabled loaders selected").

```yaml
backfill:
  key: [Company, PayablesInvoice]          # the target's join key
  columns: [PayGroup, BankTransactionCode] # the only columns written
  update_when: 'tgt.[update stamp] <= src.[update stamp]'
  set_mode: overwrite                      # overwrite | fill_null
  batch_rows: 50000                        # 0 = one statement
```

Everything else in `field_config.mapping` (typically the version stamps) is
staged into the backfill table for the gate and for traceability, and never
written to the target. Every `key`/`columns` name is checked against the mapping
*and* against the target table before anything is written.

**`update_when` is the line that matters.** These exports are point-in-time
snapshots while the daily loader keeps refreshing the same table, so without a
gate a stale snapshot would move freshly-loaded rows backwards. When the target
carries a version stamp that reaches both tables from the same export field
(`update stamp` on the payables tables), compare it: apply the snapshot only
where the target is not already ahead of it. That also makes the merge
**re-runnable** — an interrupted run is restarted from the beginning and simply
re-applies identical values.

**`set_mode`** decides how a gated row is assigned: `overwrite` (`tgt.c = src.c`)
is right *because of* the gate — a gated row is the same version the snapshot
describes, so its values are the correct ones, including the legitimately blank
ones. `fill_null` (`tgt.c = COALESCE(tgt.c, src.c)`) is the alternative for a
target with no version stamp to gate on: only the holes are filled and a value
already in the target is never touched.

> ⚠️ The merge writes to **live prod** and has no undo. Every run prints its plan
> and asks for an interactive `yes` first; `--yes` skips the prompt and
> `--dry-run` reads and reports only. `sql\payablesinvoice_header\backfill_new_columns.sql`
> carries the `SELECT ... INTO` rollback snapshot to take beforehand if you want one.

### Commands

Each chunk is one run — stage it, merge it, move on to the next:

```powershell
# Preview: prints the DDL and the MERGE, and (once the backfill table is loaded)
# how many rows would be updated. Writes nothing.
python -B backfill_helper.py --loader payablesinvoice_header_backfill `
    --file "...\invoice_additional_cols_backfill_2026.csv" --dry-run

# Stage + merge on every enabled destination, with one confirmation.
python -B backfill_helper.py --loader payablesinvoice_header_backfill `
    --file "...\invoice_additional_cols_backfill_2026.csv"
```

Split the phases to inspect the staged chunk before it touches prod — this is the
recommended shape for the first chunk of a new backfill:

```powershell
# Load the chunk into the backfill table only; prod untouched.
python -B backfill_helper.py --loader payablesinvoice_header_backfill `
    --file "...\invoice_additional_cols_backfill_2026.csv" --stage-only

# Report what the merge would do, off the data already staged (reads no file).
python -B backfill_helper.py --loader payablesinvoice_header_backfill --merge-only --dry-run

# Then merge it.
python -B backfill_helper.py --loader payablesinvoice_header_backfill --merge-only
```

`--destination des1` narrows a run to one side; `--batch-rows` overrides the
config's merge batch size (`0` merges in one statement); `--no-create` fails
instead of creating a missing backfill table. The bare config name also works in
place of `--loader <name>`.

Each run prints, per destination, how many rows were staged, how many keys were
**matched** in the target, how many passed the gate (**eligible**), and how many
were **updated**. The gap between matched and eligible is the rows the daily
loader has refreshed since the export was taken — it should be small, and those
rows should already be filled.

```
MERGED  payablesinvoice_header_backfill  des1  staged=461687  matched=461687  eligible=449764  updated=449764
```

Status is one of `STAGED`, `MERGED`, `DRY_RUN`, `SKIPPED`, or `ABORTED`.

### Backfilling a different table

Copy `configs\loaders\backfill\payablesinvoice_header.yaml`, then change: the
`name`, the two `destinations` blocks (`staging` = a new `<table>_backfill` name,
`prod` = the table being filled), `field_config.mapping` to the new export's
columns, and the `backfill:` block's `key`, `columns` and `update_when`. No code
changes — the helper reads all of it from the config. Drop each backfill table on
both servers once its last chunk is merged.

## Post-load processes

Four processes consume what the loaders land, and none depends on the others:

| Process | Config | What it runs |
| ------- | ------ | ------------ |
| `plm` | `configs\post_processes\plm.yaml` | `EXEC [PLM].[usp_RunPLM_Batch]` |
| `preprocessor` | `configs\post_processes\preprocessor.yaml` | `EXEC [Preprocessor].[usp_RunPreprocessor_Batch]` |
| `bullard_burn_down` | `configs\post_processes\bullard_burn_down.yaml` | `sp_InsertDailyArchive` per date, then rebuild `SearchTerms` |
| `payablesinvoice_vendor_gl_index` | `configs\post_processes\payablesinvoice_vendor_gl_index.yaml` | `MakePayablesInvoiceWithVendorGLIndexStg` (rebuild staging), then `UpdatePayablesInvoiceWithVendorGLIndex` (replace prod by invoice) |

`payablesinvoice_vendor_gl_index` was a monthly hand-run until it joined the daily
batch; the trailing window its staging proc rebuilds was cut from 45 days to 5 to
match the workday cadence, so a run missed for several days should be re-driven
against a widened window rather than left to catch up on its own.

`plm`, `preprocessor` and `bullard_burn_down` run on **des1** only
(`MISCPrdAdhocDB` / `PRIME`); each carries a commented `des2` block to uncomment
when that side is deployed. `payablesinvoice_vendor_gl_index` runs on **both** —
des2 is `PLMPreprocessorShared` / schema `infor` on O2, where the same two procs
carry O2's `sp_` prefix and the staging build joins `infor.MDM_VENDOR` in place of
des1's `MDM_VENDOR_INFOR` (the vendor loader lands that master under a different
name per side). Its des2 staging proc is
`sql\payablesinvoice_vendor_gl_index\des2_infor_create_make_stg_proc.sql`.

### The requirement gate

A process declares the **loaders** it needs, by loader `name`:

```yaml
requires:
  loaders: [inventory_location, inventory_transaction]
  scope: destination     # only rows written on THIS destination's server
  on_unmet: block        # record BLOCKED and execute nothing
```

Before running, it reads today's ETLHealth and requires every named loader to be
`SUCCESS`. Names resolve to the friendly `ProcessName` through the loader configs,
so the two can't drift. Because the check reads **ETLHealth** rather than the
results of the batch that just ran, an attached run and one started hours later
decide identically — and a loader you re-ran by hand clears the gate the moment it
succeeds, with no flag needed.

`scope: destination` means a des2 failure never blocks des1 work that is fine; when
a des2 block is added it gates on des2's own rows. `on_unmet: block` records one
`BLOCKED` row naming what it is waiting on, touches no table, and **leaves the exit
code at 0** — the upstream loader's own `FAILED` row is the real alarm, and counting
it twice would make one bad loader look like two broken jobs. Use
`--ignore-gate` to run anyway.

### ETLHealth: one row per process

Each process writes exactly **one** ETLHealth row per destination — the top-level
verdict, typed `TargetTableType: PROC`. The batch procs already log every
sub-procedure to their own `process_log` tables, so that detail is not duplicated;
per-step timings, row counts and the failing traceback go to the run's **log file**,
which the daily email attaches on failure.

#### `verify_process_log`: making that verdict mean something

`usp_RunPLM_Batch` and `usp_RunPreprocessor_Batch` wrap every sub-procedure in
`BEGIN CATCH` and carry on to the next one — the `THROW` is deliberately commented
out, so one bad sub-procedure never costs the whole batch. The consequence is that
the `EXEC` returns cleanly no matter what, and the rolled-up row read `SUCCESS` over
a batch that had partly failed. That bit on 2026-09-14: a PK violation in
`sp_RefreshCCXSyncedContractLine` still logged `SUCCESS`, and only turned up by
reading `process_log` by hand.

Add `verify_process_log: true` to an `exec` step to close it:

```yaml
      - name: run_preprocessor_batch
        exec: EXEC [Preprocessor].[usp_RunPreprocessor_Batch]
        timeout_seconds: 3600
        verify_process_log: true
```

After the `EXEC`, the runner reads the rows **this run** wrote to
`<database>.<schema>.process_log` and fails the step unless every one says
`Success`. The proc still runs all of its steps — only the verdict changes — and the
failing sub-procedure names go to the log while the first failure's own message
rides into ETLHealth's `Error` column, so it classifies as the real cause
(`PK VIOLATION`, `TABLE NOT FOUND`, …) rather than a bare `See Log`.

Rows are scoped by a watermark read off the **server** clock immediately before the
`EXEC`, so an earlier failure the same day — or a re-run after a fix — never re-fails
the current run. Within one run each sub-procedure logs exactly once, so "every row
since the watermark" and "the latest row per sub-procedure" are the same set; they
diverge only when two batches overlap, and there the stricter reading is what you
want, since the overlap is itself the bug.

Defaults assume the shape both batch procs use (`process_name`, `status`,
`exec_start`, `err_msg`, success = `Success`, warning = `Warning`). A mapping
overrides any of them:

```yaml
        verify_process_log:
          table: '[PLM].[audit_log]'
          ok_values: [Success, Skipped]
          warn_values: []          # [] = every non-ok status fails, as before
```

A `BLOCKED` row's `Error` column names the unmet requirement, since that is the one
detail the rolled-up row can't be read off at a glance.

#### `Warning`: a third bucket, between pass and fail

A sub-procedure that worked around something the owner should still see logs a
`Warning` row instead of `Error`. The step passes, the destination reads `SUCCESS`,
the exit code stays 0 — and the row's `err_msg` rides into that destination's
ETLHealth `Error` column, which on a `SUCCESS` row has always been a notes field.
The daily report shows any process carrying such a note even though nothing failed
(the banner stays green); the intro line above the table says so.

Anything matching neither `ok_values` nor `warn_values` is still a failure, so an
unrecognised status fails rather than slipping through.

The one producer today is `sp_RefreshCCXContractLineDeduped`, added 2026-09-17. The
GHX feed behind `CCXContractLineDeduped` keeps arriving with `LAST_UPDATE`
unpopulated — a known upstream problem with
`Contracts.dbo.ZZZ_STAGING_CCX_CONTRACTS_DETAILS_GHX_PROVIDED_DAILY` that its owner
is working through. That used to fail the whole Preprocessor batch, which is a lot of
noise for one missing column. Now the proc defaults the missing stamp to today,
refreshes normally, and logs:

```
UPDATE DATE MISSING (warning), check GHX_PROVIDED_DAILY -- 5,181,355 of 5,181,355
staging row(s) have no LAST_UPDATE; defaulted to 2026-09-17 for the price-window filter.
```

A **stale** stamp (a real date more than `@MaxFeedAgeDays` old) is still a hard
refusal — that means the feed stopped moving, which is a different problem. So is an
empty feed. Only a *missing* stamp warns.

### Commands

```powershell
# What is configured, and what each requires.
python -B run_daily_loaders.py post-run --list

# Run all three, or one by name. Without --auto it prints a summary and asks first.
python -B run_daily_loaders.py post-run --all
python -B run_daily_loaders.py post-run --process plm --auto

# Re-drive one after fixing its upstream loader (the gate re-checks and clears).
python -B run_daily_loaders.py post-run --process bullard_burn_down --auto

# Run despite an unmet requirement (deliberate override).
python -B run_daily_loaders.py post-run --process plm --auto --ignore-gate
```

`--destination`, `--log-root` and `--tag` work as they do for loaders. `--date`
moves which day's ETLHealth rows the gate checks.

### Concurrency: two axes that multiply

| Flag | Controls | Default | Why |
| ---- | -------- | ------- | --- |
| `--max-workers` | how many **processes** overlap | `1` | PLM and Preprocessor are heavy procs on the **same** server — overlapping them buys contention |
| `--destination-workers` | how many **destinations** of one process overlap | `2` | des1 and des2 are **different** servers, so there is nothing to contend over |

At most `max-workers × destination-workers` procedures run at once. The defaults
put exactly **one procedure on each server at a time**, which is the shape you
want — both servers busy, neither doubled up:

```text
des1 (PRIME):   PLM ────► Preprocessor ────► Bullard ──►
des2 (O2):      PLM ────► Preprocessor ────► Bullard ──►
                wall clock = max(des1, des2), not the sum
```

Raising **both** is what stacks several procs onto one server. `post-run` prints
the resolved product before it runs, so what you get is stated up front.

Within a destination, steps always run **sequentially** in declaration order and
the first failure stops the rest (recorded `NOT RUN` in the log). Every log line
is tagged `[des1]` / `[des2]` so concurrent destinations stay readable in the one
per-run log file.

`payablesinvoice_vendor_gl_index` is the only process with two enabled
destinations today, so it is the only one `--destination-workers` acts on; the other
three have a single destination and the pool is never larger than the work.

### Run end to end with the loaders

Add `--post-run` to a batch run. The processes run **after** the loaders and
**before** `--notify`, so a single email covers the whole night:

```powershell
python -B run_daily_loaders.py run --all --auto --post-run --notify
```

Unlike `--notify`, this is real work against prod, so a `FAILED` process **does**
make the run exit non-zero (a `BLOCKED` one does not). `--post-process <name>`
limits which ones the flag runs. A run narrowed with `--destination` to a side no
process targets yet simply skips this step with a note.

On a batch run, `--max-workers` sizes the **loader** batch only — the processes
always run one at a time there. `--destination-workers` *is* honored, so the two
servers still work in parallel inside each process.

## Daily email notification

After the daily batch, `notify` reads today's **ETLHealth** rows for the daily
loaders **and the post-load processes** and emails a report via Microsoft Graph
(service account `procurementdatateam@montefiore.org`): a one-line summary (all
success, or `X/Y` failed), a per-target status table (Process / Status / DB
Connection / Target Type / Last Run / Error, with `des1`→**PRIME** and
`des2`→**O2**), the log file(s) attached for any failure, and a consolidated
copy‑pasteable rerun command for the failed/skipped/blocked entries (entries that
share the same flags are batched into one invocation; `--prd-only` failures, full
loader reruns, and `post-run --process a b` each get their own line).

`BLOCKED` counts with the skips, not the failures — a process that never ran
because its loader failed is not a second alarm. The roster the percentages read
against (`X/Y`) includes the post-processes, so one that never ran shows under
"did not run today".
Reading ETLHealth is **read-only** — it never writes to the database. When a loader
ran more than once in the day, only its latest run is shown (and drives the
summary).

### One-time setup

1. `pip install -r requirements.txt` (adds `requests`, `cryptography`).
2. Copy `.env.example` to `.env` and fill in `TENANT_ID` / `CLIENT_ID` /
   `CLIENT_SECRET` (same app registration as A13-MedlinePBO).
3. `python first_time_setup.py` — saves the shared passphrase as the
   `E0_SECRET_PASSPHRASE` user environment variable and rewrites `CLIENT_SECRET`
   into an encrypted `CLIENT_SECRET_HASHED` line (decrypted at runtime). Open a new
   terminal afterwards so the saved variable is loaded. Optionally
   `python encrypt_env.py` produces `.env.enc` for moving the file between machines
   (`python decrypt_env.py` rebuilds `.env`).

### Adding another app's secret

Secret handling lives in [`infor_loader/env_secrets.py`](infor_loader/env_secrets.py)
and is not Graph-specific — any `.env` key can be encrypted at rest. Declare the keys
in the `SECRET_KEYS=` line inside `.env`, then encrypt them:

```powershell
# .env:  SECRET_KEYS=CLIENT_SECRET,CLIENT_SECRET_FUTURE,SFTP_PASSWORD
python -m infor_loader.env_secrets status     # which declared keys are plaintext vs encrypted
python -m infor_loader.env_secrets encrypt    # rewrite the plaintext ones as KEY_HASHED=enc::...
python -m infor_loader.env_secrets encrypt --keys SFTP_PASSWORD   # one-off, ignores SECRET_KEYS
```

The key list is resolved as `--keys` → `E0_SECRET_KEYS` env var → the `SECRET_KEYS`
line in `.env` → `DEFAULT_SECRET_KEYS` in the module. Encrypting is idempotent, so
re-running it only touches newly added plaintext keys.

Reading needs no list — `env_secrets.load_env(".env")` decrypts *every* `enc::` value
in the file and exposes `KEY_HASHED` under `KEY`, so a new secret is available with no
code change:

```python
from infor_loader.env_secrets import load_env

secrets = load_env()          # {"CLIENT_SECRET": "...", "SFTP_PASSWORD": "...", ...}
```

`notify.load_secrets` is now a thin call into `load_env`, and
[`infor_loader/msgraph.py`](infor_loader/msgraph.py) is unchanged — it still just takes
a `secrets` dict. `python -m infor_loader.env_secrets pack` / `unpack` are the whole-file
`.env` ⇄ `.env.enc` transport (what `encrypt_env.py` / `decrypt_env.py` call).

Recipients live in `configs/email.yaml` under `notification`: `test_recipients`
(used by `--mode test`, the default) and `recipients` (used by `--mode prd`). Keep
the real distribution list empty until you are ready to send widely.

### Commands

```powershell
# Read-only preview: build the report, write the HTML, send nothing.
python -B run_daily_loaders.py notify --date 2026-08-07 --dry-run --save-html out.html

# Send the report for a date to the test recipients.
python -B run_daily_loaders.py notify --date 2026-08-07 --mode test

# Send to a one-off recipient list (bypasses the config lists).
python -B run_daily_loaders.py notify --to someone@montefiore.org

# Send to the real distribution once configured.
python -B run_daily_loaders.py notify --mode prd
```

`--date` defaults to today; `--email-config` (default `configs/email.yaml`) and
`--env` (default `.env`) override the config/secret locations.

### Send automatically after the daily run

Add `--notify` to the batch to email the report when the run finishes. It is
**best-effort**: a Graph/DB/config error is reported but never changes the run's
exit code, so emailing can't fail an otherwise-successful load. `--notify-mode`
(default `test`) picks the recipient set.

```powershell
python -B run_daily_loaders.py run --all --auto --notify
python -B run_daily_loaders.py run --all --auto --notify --notify-mode prd
```

Alternatively, keep them fully decoupled and add a second Task Scheduler action
that runs `notify` after the loader action.
