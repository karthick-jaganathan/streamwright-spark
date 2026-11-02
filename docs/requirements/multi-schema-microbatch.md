# Multi-file, multi-schema microbatch processing: requirements

Status: handover to an implementing agent. Written against the code as of 2026-10-18 (the author reviewed the files named
below; line numbers are approximate). Nothing in `streamwright-spark` that needs Spark has ever been run. Treat every
Spark-facing behaviour here as "to verify", not as fact.

## 1. The question

A microbatch (one Kafka/SQS poll, up to `max_messages` messages) can name many files. Those files can differ in schema:
per-client column names, column order, extra or missing columns, different formats, different delimiters. The job must
read them in one batch, produce one canonical schema, never silently misalign columns, and tell the operator what it
dropped or rejected.

## 2. Current state (reviewed)

### Implemented: per-client column mapping, CSV/TSV only

`kinds/filesystem.py` `Filesystem.read_objects`, when `columns:` is given, uses `storage/ingest.py`:

- `plan_groups(files, mappings, headers, max_groups)` is pure Python and unit-tested (`tests/test_ingest.py`). It picks the
  mapping rows by longest `startswith` prefix, matches header cells by exact stripped text, drops unmapped columns,
  rejects a file (reason recorded) for: no mapping, empty header, two columns mapping to one name, missing required
  column, no mapped column. Files are grouped by `(prefix, header signature)`.
- `read_groups(...)` reads each group once with `header=true`, `inferSchema=false`, applies `try_cast(... AS type)` and
  an `input_file_name()` tracking column, and combines groups with `unionByName(allowMissingColumns=True)`.
- `check_vocabulary` validates mapping rows against a controlled vocabulary (names and types).
- Rejected files go to the runtime view `_file_errors` (see `engine/executor.py` `_runtime_tables`); the example job writes
  them to `bronze_rejects`.
- `check_read` requires explicit `format: csv|tsv` with `columns` (`format: auto` is rejected).
- Not exercised on Spark: `read_groups`, `rt.first_line`, `rt.exists`. The Docker seed (`docker/seed_order_files.sh`) is the first
  test data (clientA and clientB with different headers, clientC unmapped).

### Gaps

| ID | Gap | Evidence | Severity |
|----|-----|----------|----------|
| G1 | A plain read (no `columns:`) of several files assumes one schema. For CSV, Spark is expected to name columns from one header and assign the others by position, so a client with reordered or renamed columns is silently misaligned. For parquet the schema may come from one file (`mergeSchema` is off by default). **Verify the exact Spark behaviour in a spike first.** | `read_objects`, the `columns is None` branch (`spark.read...load([paths])`); `examples/kafka/order_files.yaml` step `file_contents` reads `{{ distinct_files.file_uri }}` this way and `processed_orders` assumes `order_id, client_id, item_name, amount, event_ts` | High: silent data corruption |
| G2 | The format is decided from the first path only, so a batch mixing `.csv` and `.parquet` (or `.csv` and `.json`) reads every file with the first file's format. | `read_objects`: `_format_of(paths[0], ...)` | High |
| G3 | The mapping path supports CSV/TSV only. JSON/JSONL/parquet have no rename/cast mapping. | `check_read`; `read_groups(..., "csv", ...)` hard-coded in `read_objects` | Medium |
| G4 | No schema-drift visibility. Unmapped columns are dropped silently, a client adding a column is never reported. | `plan_groups` ("Unmapped columns are dropped") | Medium |
| G5 | Bad cells become null silently (`try_cast`). `required` only checks that the header cell exists, not that values are present. No row-level error information. | `read_groups`; `plan_groups` | Medium |
| G6 | Not replay-stable. Mapping rows are pinned in the manifest (`rt.expand` -> `pinned`), but file existence (`rt.exists`) and header lines (`rt.first_line`) are not. After a crash, a replay can build different groups if a file changed or vanished. | `engine/runtime.py` (`pinned`, `exists`, `first_line`) | Medium: breaks the delivery guarantee the project promises |
| G7 | Driver-side cost is O(files) sequential Hadoop calls (`exists` per path, `first_line` per path) with S3 latency. `max_messages` defaults to 500. | `read_objects` loops | Medium |
| G8 | `unionByName` over up to `limits.read_groups` (200) frames builds a deep left-leaning plan. | `read_groups` | Low/Medium (stack depth, planning time) |
| G9 | No schema-evolution policy for the output tables: what happens when the canonical vocabulary gains a column. | `kinds/delta.py` (`options` pass-through only); `examples/kafka/order_files.yaml` line ~285 sets `mergeSchema` ad hoc | Medium |
| G10 | Only header-bearing files are supported (no headerless files, no per-client delimiter/encoding). | `read_groups` forces `header=true`; one `options` dict for the whole read | Low |

## 3. Invariants that must keep holding

These come from the project's earlier design decisions; the existing tests protect some of them.

1. **No extra Spark actions.** Steps stay lazy; each output does one action (its write); counts come from `observe` or
   commit metrics. Driver-side Hadoop FileSystem calls (listing, header lines) are allowed and are listed by
   `sw spark validate --analyze` as "driver reads". Do not add `count()`, `collect()` or `show()` on data frames.
2. **Replay stability.** Anything that decides what is read (file set, header signatures, grouping) is computed once per
   batch and pinned in the manifest (`manifest["pinned"]`), then reused on replay.
3. **No provider names in the engine.** The object read/write behaviour lives once in `kinds/filesystem.py` and
   `storage/`; connectors (in the separate `streamwright` repo) only state facts. Do not edit the streamwright repo for
   this work.
4. **Planning is pure Python and testable without Spark.** Only the code that builds data frames may import pyspark, and
   only lazily inside functions.
5. **Credentials** are only `{{ secrets.NAME }}` references; do not log values.
6. **Python 3.10, pyenv env `streamwright`.** Tests: `make test PYTHON=python3` (uses `unittest`). pytest and pyspark may
   not be installed; Spark-dependent tests must skip cleanly when `pyspark` is missing.
7. **Do not run `git commit`.** Mark new behaviour `PROPOSED` in docstrings until it has run on Spark.

## 4. Requirements

Priorities: P0 must ship, P1 should, P2 later. Each requirement lists acceptance criteria (AC).

### R1 (P0) Safe plain read: group by header, never read by position (fixes G1)

Without `columns:`, a multi-file read of CSV/TSV must not rely on one file's header for all files.

- New argument `schema_mode` on `object.read` / `file.read`: `by_name` (default) or `strict`.
- `by_name`: read the header of each file on the driver (see R7/R6 for how), group files by `(format, delimiter options,
  header signature)`, read each group with `header=true`, and combine groups with `unionByName(allowMissingColumns=True)`.
  All columns stay strings unless the job casts them (inference is off). Column names are the header cells, stripped.
- `strict`: all files must share one header signature; files that differ are rejected into `_file_errors` (reason names
  the first differing column), subject to `on_mismatch` (`skip` | `error`, the existing argument; default stays `error`).
- Parquet and JSON/JSONL under `by_name`: group by the file's own schema (parquet footer / json inferred schema from a
  bounded sample), read per group, union by name. A column that has different types in different groups is a conflict:
  `on_type_conflict: string | reject` (default `reject` the later files to `_file_errors`; `string` casts both to string).
- AC: unit tests (pure Python) for grouping and for `strict` rejects; a Spark test (skipped without pyspark) with two CSV
  files of the same columns in a different order, and one with a renamed column, proving values land in the right column
  under `by_name` and that `strict` rejects the odd file. The Spark-behaviour spike for G1 is documented in the test.
- AC: `sw spark validate` warns when a multi-file read has no `columns:` and no explicit `schema_mode` ("columns are
  matched by name; set schema_mode: strict to reject differing files").

### R2 (P0) Per-file format (fixes G2)

- With `format: auto`, resolve the format per file from its extension (existing `_format_of` logic); group by format; read
  each group with its own reader; combine by name (R1 rules).
- An explicit `format` applies to all files and overrides the extension.
- A file whose format cannot be determined is rejected into `_file_errors`, not a batch failure (unless `on_mismatch:
  error`).
- AC: pure-Python test of the per-file resolution and grouping; Spark test reading one `.csv` and one `.parquet` in one
  batch.

### R3 (P1) Mapping for JSON/JSONL/parquet (fixes G3)

- `columns:` mapping rows today are `(prefix, header, canonical, type, required)`. For non-CSV formats `header` is the
  top-level field name (dotted paths like `a.b` may be supported for JSON; document the choice).
- Reuse `plan_groups` by letting the caller supply the field list per file (a `FieldReader` seam): CSV header from
  `first_line`; parquet from the footer schema; JSON from a bounded sample (default first 100 lines, configurable).
- Remove the CSV-only restriction in `check_read`; keep `format` explicit when `columns` is used.
- AC: tests mirroring `tests/test_ingest.py` for fields from json and parquet; one Spark test per format.

### R4 (P0) Schema-drift visibility (fixes G4)

- New runtime view `_schema_report` (reserve the name in `job/check.py` `RESERVED_NAMES`), one row per file read:
  `(step STRING, path STRING, prefix STRING, status STRING, header_hash STRING, mapped ARRAY<STRING>,
  unmapped ARRAY<STRING>, missing_optional ARRAY<STRING>)`, status `ok | drift | rejected`.
- `drift` means: the file's header signature differs from the most recent previously accepted signature for the same
  prefix (store the last accepted signature per prefix in a small table in the bookkeeping store, schema `sw_ops`,
  so the comparison survives batches), or it has unmapped columns.
- New argument `on_unmapped_column: ignore | warn | reject` (default `warn`): `warn` logs once per `(prefix, header_hash)`
  per batch and records the row in `_schema_report`; `reject` sends the file to `_file_errors`.
- The batch summary (`--summary`) includes counts: files read, rejected, drifted, distinct signatures.
- AC: unit tests for the status logic; Docker seed extended (section 6) and the job writes `_schema_report` to a
  `bronze_schema_report` Delta/Postgres output in `examples/bronze_client_orders.yaml`.

### R5 (P1) Row-level quality without extra actions (fixes G5)

- When `row_errors: true` on a mapped read, add a column `_row_errors ARRAY<STRING>`: canonical names whose value was
  non-empty in the source but null after `try_cast` (cast failure), plus required columns that are null. Pure column
  expressions; no action.
- Job authors route rows in SQL (for example `WHERE size(_row_errors) = 0` vs the rest). Document this in the cookbook.
- Optional (design before building): per-column counts of cast failures through `observe` attached to the data frame
  that the first write consumes, reported in the write receipt. Note the limitation when the frame is persisted or
  checkpointed (metrics fire only on first computation). If this cannot be done without an extra action, skip it.
- AC: pure-Python test of the expression builder (string output); Spark test with a bad date and a missing required value.

### R6 (P0) Replay-stable ingest plan (fixes G6)

- After planning a read step, store the plan in `manifest["pinned"]["ingest:<step>"]`: for each file its status, the
  header (or field list), the chosen prefix and the group id; plus the rejects. On replay, rebuild groups from the pinned
  plan and do not call `exists` / `first_line` again.
- If the pinned plan names a file that has since disappeared, the Spark read fails; the error must name the file and say
  the batch is replaying a pinned plan.
- AC: unit test using a fake runtime: plan once, mutate the "file system", plan again with the pinned manifest, expect
  identical groups; a second test that fresh batches plan from scratch.

### R7 (P1) Driver-side scaling (fixes G7)

- Run `exists` and header reads with bounded parallelism: new `runtime.limits.listing_parallelism` (default 16, max 64).
  Keep the output deterministic (sort results by path before planning).
- Prefer one `FileSystem.listStatus` / `globStatus` call per common parent over one `exists` per file where paths share a
  parent; fall back to per-file calls.
- New `runtime.limits.max_files_per_batch` (default 2000); exceeding it fails the batch with a clear message before any
  read.
- Log the count and elapsed milliseconds of the driver phase (one event, no secrets).
- AC: unit test with a fake file system proving the call count, ordering and that failures are isolated per file.

### R8 (P2) Balanced union (fixes G8)

- Combine group frames with a balanced pairwise union instead of a left fold, so plan depth is O(log n).
- AC: unit test on the combiner with fake frames recording depth.

### R9 (P2) Output schema evolution policy (fixes G9)

- New argument `schema_evolution: strict | add_columns` on `tables.append`, `tables.overwrite` and `tables.merge`
  (default `strict`). `add_columns` sets Delta's `mergeSchema` on append/overwrite and the auto-merge setting for merge.
  **Verify** how auto-merge is scoped for `MERGE` in Delta 3.2.1 (session configuration is global; do not leak it to other
  outputs: set and restore around the write, or use the builder option if one exists).
- `sw spark validate` warns when a mapped read feeds a Delta write with `strict` and the vocabulary was extended.
- AC: Spark test appending a frame with one extra column in both modes.

### R10 (P2) Headerless files and per-client read options (fixes G10)

- Optional mapping-row columns `delimiter`, `encoding`, `has_header` (default true). A headerless file uses positional
  names `_c0, _c1, ...` as the `header` value in the mapping. Grouping adds these options to the group key.
- AC: pure-Python grouping tests; Spark test with a semicolon-delimited headerless file.

### R11 (P0) Documentation and examples

- `README.md` and `cookbook/` get a "multi-schema batches" page: the three modes (plain by name, strict, mapped), the
  runtime views `_file_errors` and `_schema_report`, the limits, and the replay rule.
- Update `examples/kafka/order_files.yaml` so `file_contents` states `schema_mode` explicitly, and add the
  required-columns check it relies on.

## 5. Suggested order

1. G1 spike (document what Spark actually does for CSV and parquet with mixed schemas) -> R1, R2.
2. R6 (plan pinning) together with R1/R2, because it changes where planning lives.
3. R4 (reports), R11 (docs). 4. R7. 5. R3, R5. 6. R8, R9, R10.

## 6. Test data and expected results (extend `docker/seed_order_files.sh`)

Keep the existing five messages and add (new files under `acme-landing`):

| File | Content | Expected |
|------|---------|----------|
| `clientA/orders_20261018.csv` | same headers as clientA's mapping but columns in a different order | mapped by name, 2 rows, status `ok` |
| `clientA/orders_20261019.csv` | clientA headers plus an unmapped column `promo_code` | rows loaded, status `drift`, `unmapped = [promo_code]` |
| `clientB/orders_20261018.csv` | `Revenue` contains `n/a` on one row | 1 row with `revenue` null and `_row_errors = [revenue]` (R5) |
| `clientE/events.parquet` and `clientE/events.csv` in one message batch | two formats | both read (R2); a mapping for clientE is added to `docker/postgres-init.sql` |

The acceptance run: `make down && make up && make seed-order-files`, then the documented `docker compose run --rm sw run ... --drain`;
afterwards `sw spark state show` shows one completed batch and the report outputs contain the rows above. Report any
deviation; do not adjust the expectations to match the output without saying so.

## 7. Open questions (defaults to use if nobody answers)

1. Default `schema_mode` for a plain multi-file read: `by_name` (recommended). `strict` is safer but rejects more.
2. Where the "last accepted signature per prefix" lives: a table in the bookkeeping schema (recommended) vs a Delta table.
3. Should `_schema_report` be written by default by the example job (recommended: yes, to Postgres `reporting`).
4. Type conflicts across files (R1): `reject` (recommended) vs `string`.
5. JSON sample size for field discovery (default 100 lines).

## 8. Out of scope

Streaming (Structured Streaming) sources, schema registries, Avro/ORC, nested-field mapping beyond one dotted level,
changes to the streamwright connectors or hub, the EMR launcher.

## 9. How to work on this

- Code to touch: `src/streamwright/spark/storage/ingest.py` (planning and reads), `src/streamwright/spark/kinds/filesystem.py`
  (`read_objects`, `check_read`, argument tables), `src/streamwright/spark/engine/runtime.py` (pinning, driver reads),
  `src/streamwright/spark/engine/executor.py` (`_runtime_tables`), `src/streamwright/spark/job/check.py` (reserved
  names, validation warnings), `src/streamwright/spark/plan/graph.py` (`--analyze` text for driver reads).
- Tests: `tests/test_ingest.py` and new test modules; run `make test PYTHON=python3` from the repository root.
- Finish with a short report: what is implemented, what is covered by which test, what only ran on fakes, and every place
  you were unsure. Do not claim Spark behaviour you did not run.
