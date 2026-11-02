# Multi-Schema Microbatch Ingestion & Quality Routing

This guide covers StreamWright Spark's multi-file, multi-schema microbatch architecture. It explains how StreamWright eliminates Spark's native column misalignments, tracks schema drift across client uploads, enforces replay stability, and routes corrupted rows without extra Spark actions.

---

## 1. The Challenge: Native Spark Positional Misalignment

In high-throughput event-driven microbatches (consuming SQS or Kafka arrival notifications), a single batch can process tens or hundreds of files. When files arrive from multiple third-party clients or systems, they often vary in:
- Column order (e.g., File 1 has `order_id, amount`, File 2 has `amount, order_id`).
- Formats (mixing `.csv` and `.parquet` in a single microbatch).
- Added or dropped columns (schema drift).

### Native Spark Behavior (G1)
When reading multiple CSV files via `spark.read.format("csv").option("header", "true").load([f1, f2])`:
1. Apache Spark takes the column names strictly from the **first file** inspected.
2. Subsequent files are read **by position**, silently placing values into incorrect columns!
3. For Parquet, Spark inspects one file's footer schema and drops unmerged columns by default.

StreamWright Spark completely eliminates this vulnerability through driver-side planning and balanced pairwise unions.

---

## 2. Ingest Modes

StreamWright supports three ingest modes for `object.read` and `file.read`:

### Mode A: Plain Read By Name (`schema_mode: by_name`, Default)
Groups files by format, delimiter options, and exact header signature. Each group is read with its native schema and combined using balanced pairwise `unionByName(allowMissingColumns=True)`.

```yaml
  - name: incoming_orders
    connection: landing_s3
    service: object
    method: read
    arguments:
      path: "{{ arrivals.payload.file_uri }}"
      format: auto
      schema_mode: by_name
      options:
        header: true
```
- **Order Preservation**: Files with differing column order land in distinct groups, perfectly aligned by column name.
- **Extra Columns**: Extra columns in any file are preserved and filled with `NULL` for files lacking them.
- **Inference Disabled**: Columns remain strings until explicitly cast in SQL steps.

### Mode B: Plain Strict Read (`schema_mode: strict`)
Enforces that every file in the batch shares the exact header signature as the baseline file.

```yaml
  - name: partner_events
    connection: landing_s3
    service: object
    method: read
    arguments:
      path: "{{ arrivals.payload.file_uri }}"
      format: csv
      schema_mode: strict
      on_mismatch: skip         # "skip" routes to _file_errors; "error" halts the batch
      options:
        header: true
```
- Any file with reordered, missing, or extra columns is rejected into the `_file_errors` view with an explanatory reason (e.g., `schema mismatch: differing column at position 1: 'amount' (expected 'order_id')`).

### Mode C: Per-Client Mapped Read (`columns:` & `vocabulary:`)
Harmonizes diverse client schemas into a single canonical vocabulary. Supports exact matching as well as regular expression matching on both file paths and header names:

```yaml
  - name: harmonized_orders
    connection: landing_s3
    service: object
    method: read
    arguments:
      path: "{{ arrivals.payload.file_uri }}"
      format: auto
      on_unmapped_column: warn  # ignore | warn | reject
      on_mismatch: skip
      row_errors: true          # adds _row_errors ARRAY<STRING>
      # Named regex extraction: extracts variables from URI and auto-injects them as DataFrame columns!
      path_pattern: "partners/(?P<partner>[^/]+)/"

      vocabulary:
        - {name: order_id,    type: bigint}
        - {name: customer_id, type: string}
        - {name: amount,      type: double}

      # Option 1: Dict-based mapping grouped by extracted variable
      columns:
        by: partner

        alpha:
          - regex: "(?i)^order[_\s]?(num|id|no)$"
            canonical: "order_id"
            type: "bigint"
            required: true
          - header: "AmountUSD"
            canonical: "amount"
            type: "double"
            required: true

        beta:
          - regex: "(?i)^txn[_\s]?id$"
            canonical: "order_id"
            type: "bigint"
            required: true
          - header: "total"
            canonical: "amount"
            type: "double"
            required: true

      # Option 2 (Alternative): Traditional flat list with prefix: or path_regex:
      # columns:
      #   - prefix: "s3://landing/partners/alpha/"
      #     header: "Order_Num"
      #     canonical: "order_id"
      #     type: "bigint"
      #     required: true
      #   - path_regex: ".*/inbox/.*_beta_.*\\.csv$"
      #     regex: "(?i)^txn[_\s]?id$"
      #     canonical: "order_id"
      #     type: "bigint"
      #     required: true
```
- **Named Regex Extraction (`path_pattern`)**: Extracts variables (such as `partner`) and automatically projects them onto the DataFrame (`h.partner`).
- **Dict Dispatch (`by: <variable>`)**: Dispatches files to schema mapping groups directly based on the extracted variable, avoiding repetitive `prefix:` declarations.
- **Header Matching**: Exact `header:` matches are checked first ($O(1)$); if unmatched, `regex:` patterns are evaluated. If multiple columns collide into the same canonical target, StreamWright detects it and rejects the file.
- **Pre-Read Mapping**: All column transformations happen at the reader projection layer *before* any union, keeping Spark DataFrames lean and preventing sparse schema explosions.
- **Tracking Column**: The source file path is automatically attached as `_source_file` on every row via `input_file_name()`. Auto-injected path variables (like `h.partner`) eliminate the need for downstream `regexp_extract(_source_file, ...)` expressions.

### Mode D: Pattern-Based Column Folding (`column_groups:`, PROPOSED)
In many ingestion workloads (e.g. IoT telemetry, server metrics, ad tracking, or dynamic client attributes), files arrive with dozens of dynamic or sparse columns (such as `metric_cpu`, `metric_memory`, `metric_disk_read`, `tag_region`). Rather than constantly evolving the table schema with hundreds of sparse columns, `column_groups:` folds matching columns into typed `MAP<STRING, TYPE>` columns using explicit **wildcard globs** (`glob:`) or **regular expressions** (`regex:`):

```yaml
  - name: telemetry_events
    connection: landing_s3
    service: object
    method: read
    arguments:
      path: "{{ arrivals.payload.file_uri }}"
      format: csv
      schema_mode: by_name
      column_groups:
        # Wildcard glob matching (e.g. metric_cpu -> cpu)
        - name: metrics
          glob: "metric_*"
          type: double
          strip_prefix: "metric_"    # optional: strips prefix from map key

        # Regular expression matching (e.g. tag_alpha -> tag_alpha)
        - name: tags
          regex: "^tag_[a-z0-9]+$"
          type: string
          # strip_prefix omitted: map key remains full column name
```

Key guarantees of `column_groups:`:
- **Explicit Matching**: Each group declares either `glob:` (Unix wildcard) or `regex:` (Python regular expression). Both cannot be declared simultaneously.
- **Typed Spark Maps**: Assembled via `map_from_entries(...)` with `try_cast` for values into `MAP<STRING, TYPE>`.
- **Null Pruning**: Null entries are pruned automatically via `map_filter(...)` so rows only contain present keys (set `keep_nulls: true` to preserve null entries).
- **Optional Prefix Stripping**: `strip_prefix: "metric_"` is purely optional. When provided, it strips the prefix so map keys become clean identifiers (e.g. `'cpu'` instead of `'metric_cpu'`). When omitted, the original column name is preserved as the map key.
- **Schema Drift Protection**: Columns matched by `column_groups` are treated as mapped in `_schema_report`, preventing false drift alerts.
- **Empty Group Support**: Files with no matching columns for a group emit `cast(map() as map<string, TYPE>)`, ensuring safe `unionByName`.

### Mode E: Lazy File Row Numbering (`row_index:`)
Tracking the exact row number (or line offset) within each input file is critical for auditing, line-level error tracing, and data reconciliation. However, writing `row_number() OVER (PARTITION BY _source_file ORDER BY monotonically_increasing_id())` in downstream SQL triggers an expensive Catalyst window shuffle across the entire cluster.

StreamWright Spark solves this by attaching the row numbering projection **lazily directly at file-read time**:
```yaml
  - name: harmonized_feed
    connection: landing_s3
    service: object
    method: read
    arguments:
      path: "{{ arrivals.payload.file_uri }}"
      format: auto
      # Options:
      # 1. Default toggle (emits `_row_index BIGINT`, 0-indexed):
      # row_index: true
      # 2. String alias (emits `row_id BIGINT`, 0-indexed):
      row_index: "row_id"
      # 3. Custom name and 1-indexed line offset:
      # row_index: {name: "line_number", start: 1}
```

Key guarantees of `row_index:`:
- **Zero Shuffle / 100% Lazy**: The window expression `row_number().over(Window.partitionBy(origin).orderBy(F.monotonically_increasing_id()))` is attached directly during the file group's initial projection before any union. No eager actions are triggered on the driver.
- **Per-File Reset**: Partitioned by the physical input file URI (`origin = F.input_file_name()`), ensuring row indices reset to `0` (or `1`) for every individual file in the batch.
- **Deterministic Multi-Partition Reading**: Because `monotonically_increasing_id()` assigns the partition index in the upper 31 bits and record offset in the lower 33 bits, ordering by this ID preserves the physical file line sequence across split partitions.
- **Custom Aliasing & Start Offset**: Emits `_row_index` by default with `true`, or any custom identifier string (e.g. `row_id`), and supports 1-based indexing via `{name: ..., start: 1}`.

---

## 3. Per-File Format Resolution (`format: auto`)

When `format: auto` is specified, StreamWright inspects each file's extension individually:
- Supported: `.csv`, `.tsv`, `.parquet`, `.json`, `.jsonl` (and compressed variants like `.csv.gz`, `.parquet.snappy`).
- Files are grouped by format and read with their respective format readers.
- Files with unknown extensions are rejected into `_file_errors` without crashing other valid files in the batch.

---

## 4. Replay-Stable Plan Pinning

To uphold StreamWright's once-and-only-once delivery and idempotency guarantees:
1. **Batch Receipt**: On fresh execution, the driver plans file formats, reads headers, builds groups, and identifies rejects.
2. **Manifest Pinning**: The entire ingest plan is serialized into `manifest["pinned"]["ingest:<step>"]`.
3. **Replay / Recovery**: If a worker crashes or a batch is retried, `StepRuntime` reconstructs groups and rejects directly from the pinned manifest without re-querying S3/HDFS.
4. **Disappeared File Guard**: If a file present in the pinned manifest was deleted from storage before replay, StreamWright fails fast with a clear message:
   `StorageError: batch is replaying a pinned plan, but file 's3://...' has disappeared`.

---

## 5. Schema Drift Visibility (`_schema_report` View)

Every file evaluated in a microbatch generates a row in the internal temporary view `_schema_report`:

| Column | Type | Description |
|---|---|---|
| `step` | `STRING` | Step name that executed the read |
| `path` | `STRING` | Source file URI |
| `prefix` | `STRING` | Matched client mapping prefix (or empty) |
| `status` | `STRING` | `ok`, `drift`, or `rejected` |
| `header_hash` | `STRING` | SHA-256 hash of the file's header cells |
| `mapped` | `ARRAY<STRING>` | Canonical column names mapped |
| `unmapped` | `ARRAY<STRING>` | Source header columns with no mapping |
| `missing_optional` | `ARRAY<STRING>` | Optional canonical columns absent in this file |

### Drift Detection
A file receives `status: drift` if:
1. It contains unmapped columns (under `on_unmapped_column: warn`).
2. Its `header_hash` differs from the most recently accepted signature stored in `sw_schema_signatures` for that client prefix.

### Querying Schema Reports in SQL
```yaml
outputs:
  - name: audit_drift
    connection: ops_db
    service: database
    method: write
    arguments:
      table: schema_drift_log
    sql: |
      SELECT step, path, prefix, status, header_hash,
             array_join(unmapped, ',') AS unmapped_columns,
             :batch_id AS batch_id, current_timestamp() AS detected_at
      FROM _schema_report
      WHERE status = 'drift'
```

---

## 6. Row-Level Quality Routing (`row_errors: true`)

Setting `row_errors: true` appends a virtual column `_row_errors ARRAY<STRING>` containing the canonical names of fields that failed casting or were null when marked `required`.

Because this uses pure SQL column expressions (`try_cast`, `when`, `array_compact`), it adds **zero extra Spark actions**!

### Filtering Clean vs. Corrupted Rows
```sql
-- Step 5: Clean rows for Delta merge
SELECT * FROM harmonized_orders
WHERE size(_row_errors) = 0;

-- Output 6: Quarantine dead-letter rows to S3
SELECT *, array_join(_row_errors, '; ') AS failure_reasons
FROM harmonized_orders
WHERE size(_row_errors) > 0;
```

---

## 7. Delta Lake Schema Evolution Policy

When writing to Delta Lake, StreamWright provides explicit control over schema changes via `schema_evolution`:

```yaml
outputs:
  - name: orders_delta
    connection: lake_delta
    service: table
    method: write
    arguments:
      mode: merge
      path: "s3://{{ config.lake_bucket }}/delta/orders"
      schema_evolution: add_columns   # strict (default) | add_columns
      condition: "target.order_id = source.order_id"
      # ...
```

- **`strict` (default)**: Rejects mismatched schemas to protect downstream analytics consumers.
- **`add_columns`**:
  - `append` / `overwrite`: Enables Delta's `mergeSchema` option.
  - `merge`: Dynamically activates `spark.databricks.delta.schema.autoMerge.enabled` scoped strictly to that merge execution, restoring the previous setting afterwards to avoid session leakage.

---

## 8. Runtime Limits & Batch Summary

Configure bounds in the `runtime:` section:
```yaml
runtime:
  limits:
    read_groups: 200              # Maximum allowed distinct header signatures
    max_files_per_batch: 2000     # Batch file count ceiling
    listing_parallelism: 16       # Driver threads for header/exists checks
```

### Inspecting Summary Statistics
Run with `--summary` to print ingest metrics upon batch completion:
```bash
sw spark run job.yaml --once --summary
```
Output:
```text
Batch kafka_order_files-00000001 summary: 148 read, 2 rejected, 3 drifted, 4 distinct signatures
```

---

## 9. Multi-Secret & Plaintext Resolution (`runtime.secrets:`)

Production pipelines often require multiple distinct secrets (e.g., separate database credentials for operational state stores and analytical reporting databases, or third-party API tokens). In AWS Secrets Manager, secrets are commonly provisioned either as **single-value raw plaintext strings** or as **JSON key-value dictionaries**.

StreamWright handles both seamlessly under `runtime.secrets.aws_secretsmanager:`:

```yaml
runtime:
  secrets:
    aws_secretsmanager:
      region: "us-east-1"
      # Mapping declared secrets to specific AWS Secrets Manager Secret IDs / ARNs:
      mapping:
        ops_db_password: "prod/streamwright/ops_db"       # Resolved from raw plaintext or JSON {"ops_db_password": "..."}
        audit_db_password: "prod/streamwright/audit_db"   # Resolved from raw plaintext or JSON {"audit_db_password": "..."}
        pg_password: "prod/rds/credentials"               # Extracts JSON key "pg_password" if secret is JSON
```

- **Plaintext vs JSON Detection**: If the retrieved AWS Secret string begins with `{` and parses as valid JSON, StreamWright extracts the matching key. If it is raw plaintext (e.g. `supersecret123`), StreamWright assigns the full string directly.
- **Multi-Secret Merging**: All resolved secrets are merged into a single dictionary accessible in Jinja expressions (`{{ secrets.ops_db_password }}`).
- **Local Overrides**: For local testing, set `runtime.secrets: null` in your overlay, and StreamWright will automatically resolve `STREAMWRIGHT_SECRET_<NAME>` from your local environment variables.

---

## 10. Declarative Environment Mapping & Dynamic EMR Cluster Targeting

### A. Automatic Container Environment Injection (`runtime.env:`)
Distributing environment variables to Spark drivers and executors traditionally requires cumbersome Spark configuration flags. StreamWright allows declaring environment variables cleanly under `runtime.env:`:

```yaml
runtime:
  env:
    STREAMWRIGHT_SECRET_OPS_DB_PASSWORD: "{{ secrets.ops_db_password }}"
    STREAMWRIGHT_SECRET_AUDIT_DB_PASSWORD: "{{ secrets.audit_db_password }}"
    APP_ENV: "production"
    LOG_LEVEL: "INFO"
```

During job launch, StreamWright maps non-secret variables to:
1. `spark.yarn.appMasterEnv.<KEY>` (for the YARN ApplicationMaster container)
2. `spark.executorEnv.<KEY>` (for all distributed Spark executor containers)
3. `os.environ[<KEY>]` (on the driver)

`STREAMWRIGHT_SECRET_*` values are resolved only on the driver and never copied into
Spark configuration. Cluster drivers fetch them using `runtime.secrets`.

### B. Resilient Cluster Targeting via CloudFormation (`stack_name:` / `stack_arn:`)
In ephemeral cloud environments, EMR clusters are periodically terminated and reprovisioned by Infrastructure-as-Code (IaC) pipelines, causing physical cluster IDs (`j-XXXXXXXXXXXX`) to change.

StreamWright allows targeting EMR clusters directly by CloudFormation Stack Name or ARN:

```yaml
runtime:
  platform:
    type: emr_ec2
    cluster:
      stack_name: "prod-data-platform-emr-stack"    # Or stack_arn: "arn:aws:cloudformation:..."
    region: "us-east-1"
```

StreamWright queries AWS CloudFormation describe-stack resources to dynamically discover the active, running EMR cluster ID before submitting the Spark step, making CI/CD workflows and automated schedulers resilient against cluster recreations.
