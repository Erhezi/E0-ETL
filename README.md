# Infor File Loaders

The loader package is driven by one YAML file per data source under
`configs\loaders\`. The `configs\` root holds the shared
`file_folder_loader_config.yaml` registry (see "Input files" below); everything
else in `configs\loaders\` is a loader:

```text
configs\
  file_folder_loader_config.yaml   # input-file registry (not a loader)
  loaders\
    inventory_location.yaml        # one YAML per data source
    ...
```

Commands still take `--config configs` (the default): the loaders are read from
`configs\loaders\` automatically, and the registry is found at the `configs\`
root.

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

Run selected loaders in parallel once more YAML files are added:

```powershell
python -B run_daily_loaders.py --loader inventory_location --loader other_loader --auto --max-workers 4
```

For Windows Task Scheduler, set the working directory to this folder and use the
`--auto` form above (or the equivalent `python -B run_daily_loaders.py run ...`
subcommand, which never prompts).

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
| `FILE NOT FOUND` | the source file could not be located |
| `COLUMN NOT FOUND` | an expected **loaded** source column is absent from the file |
| `PK VIOLATION` | primary-key duplicate (SQL insert or the app-side `pk_check`) |
| `UX VIOLATION` | unique key / unique index violation |
| `FIELD TRUNCATE` | a source value exceeded the destination column's length |
| `See Log` | anything else — open `LogFilePath` for detail |

A failed step stops that destination's remaining steps; other destinations still run.

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

The bare config name also works in place of `--loader <name>`
(e.g. `python -B data_fill_helper.py --inventory_location`). On completion it
prints a status line, e.g. `COPIED  inventory_location  rows_truncated=120  rows_copied=120`;
status is one of `COPIED`, `DRY_RUN`, `SKIPPED_NONEMPTY`, or `ABORTED`.
