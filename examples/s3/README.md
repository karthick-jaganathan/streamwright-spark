# Multi-Partner Schema Harmonization & Quality Routing Pipeline

A declarative, production-grade batch ingestion pipeline executed on **StreamWright Spark**:

```
[Heterogeneous Partner Files (Amazon S3)]
├── s3://landing/partners/alpha/alpha_orders_01.csv   (Headers: Order_Num, Cust_ID, AmountUSD, EventDate, promo_code)
├── s3://landing/partners/beta/beta_orders_01.tsv     (Headers: transaction_id, client_key, total, timestamp)
└── s3://landing/partners/unknown/rejected_01.csv     (Unrecognized partner path)
                                │
                                ▼
        [STEP 1: harmonized_feed (StreamWright Object Read)]
        ├── 1. Embedded Prefix Mapping: Matches longest URI prefix (s3://.../partners/<name>/)
        ├── 2. Embedded Vocabulary Check: Validates target names & types against enterprise dictionary
        ├── 3. Header Inspection: Groups matching files into unified reader clusters
        ├── 4. Zero-Overhead Quality: Builds _row_errors via try_cast expressions
        └── 5. Schema Drift: Compares header hash against sw_schema_signatures
                                │
                                ▼
        [STEP 2: harmonized_orders (PySpark Checkpoint & Cache)]
                                │
        ┌───────────────────────┴───────────────────────┐
        ▼ (size(_row_errors) = 0)                       ▼ (size(_row_errors) > 0)
[OUTPUT 1: Delta Lake Merge]                 [OUTPUT 2: S3 Dead-Letter Quarantine]
- Target: Partitioned Delta Lake Table       - Target: s3://lake/quarantine/orders/
- Condition: target.order_id = source.id     - Format: JSON Lines (.jsonl)
- schema_evolution: add_columns              - Columns: original record + failure_reasons
- Target Predicate File Pruning              - Zero data loss for corrupted records!
        │
        ├───────────────────────────────────────────────┐
        ▼                                               ▼
[OUTPUT 3: Schema Drift Audit Log]           [OUTPUT 4: File Rejection Audit Log]
- Source: _schema_report view                - Source: _file_errors view
- Target: Postgres audit.schema_drift_events - Target: Postgres audit.file_rejection_events
- Action: Alert on new unmapped columns      - Action: Alert on unrecognized partner paths
```

---

## The Enterprise Challenge: Heterogeneous Partner Ingestion

In enterprise and B2B platforms, data rarely arrives in a uniform schema:
- **Partner Alpha** sends `.csv` files with headers `Order_Num`, `Cust_ID`, `AmountUSD`.
- **Partner Beta** sends `.tsv` files with headers `transaction_id`, `client_key`, `total`.
- **Unknown Partners** drop files in unexpected folders.
- Files arrive unpredictably in mixed microbatches containing dozens or hundreds of files.

### Traditional Anti-Patterns
1. **Ad-hoc Glue Scripts**: Writing custom Python/Pandas parsers for each partner that frequently crash when headers vary.
2. **Native Spark Positional Misalignment**: Calling `spark.read.csv()` on multi-file batches causes Spark to silently read files **by column position**, corrupting data across differently ordered headers.
3. **Expensive Row-by-Row Python UDFs**: Running validation row-by-row in Python kills Spark cluster throughput and explodes memory usage.

---

## Architectural Principles & Invariants in this Pipeline

### 1. Named Regex Path Extraction & Dict-Based Schema Mapping (`path_pattern:` & `columns:`)
Mapping rules can be cleanly structured as a dictionary keyed by partner name, without repeating full URI prefixes:
```yaml
      path_pattern: "partners/(?P<partner>[^/]+)/"
      columns:
        by: partner

        # Partner Alpha (CSV): Order_Num, Cust_ID, AmountUSD, EventDate
        # Uses regex: to match casing or formatting variants (Order_Num, order_id, Order #)
        alpha:
          - regex: '(?i)^order[_\s]?(num|id|no)$'
            canonical: "order_id"
            type: "bigint"
            required: true
          - header: "Cust_ID"
            canonical: "customer_id"
            type: "string"
          - header: "AmountUSD"
            canonical: "amount"
            type: "double"
            required: true
          - header: "EventDate"
            canonical: "event_ts"
            type: "timestamp"

        # Partner Beta (TSV): transaction_id, client_key, total, timestamp
        # Uses regex: to match transaction_id or txn_id variations
        beta:
          - regex: '(?i)^(transaction|txn)[_\s]?(id|num)$'
            canonical: "order_id"
            type: "bigint"
            required: true
          - header: "client_key"
            canonical: "customer_id"
            type: "string"
          - header: "total"
            canonical: "amount"
            type: "double"
            required: true
          - header: "timestamp"
            canonical: "event_ts"
            type: "timestamp"
```
- **Named Regex Extraction**: `path_pattern: "partners/(?P<partner>[^/]+)/"` extracts variables from the file URI.
- **Selector Dispatch (`by: partner`)**: The extracted variable dispatches directly to the corresponding schema rules (`alpha`, `beta`). Composite selectors like `by: "{{ client }}/{{ feed }}"` are also supported.
- **Automatic Column Projection**: Any named groups extracted by `path_pattern` are **automatically injected into the PySpark DataFrame** as first-class projected columns!
- **Flexible Header Matching**: Rules support exact `header:` matching as well as pattern-based `regex:` matching with collision detection.
- **Backward Compatible**: Traditional flat lists with `prefix:` or `path_regex:` continue to work unchanged.
- **Dynamic Database Alternative**: In large organizations with thousands of partners, `columns:` can also reference a database step: `columns: "{{ partner_schema_registry }}"`.

### 2. Pattern-Based Dynamic Column Folding (`column_groups:`)
Rather than evolving the table schema with dozens of sparse partner columns, dynamic metrics and extra attributes are folded into typed Spark Map columns:
```yaml
      column_groups:
        # Folds dynamic metric columns into MAP<STRING, DOUBLE>
        - name: metrics
          regex: "^(fee|metric)_[a-z0-9_]+$"
          type: double
          strip_prefix: "metric_"

        # Folds dynamic partner attributes into MAP<STRING, STRING>
        - name: extra_attributes
          glob: "attr_*"
          type: string
          strip_prefix: "attr_"
```
- **Vocabulary Independence**: Dynamic columns folded by `column_groups:` **do not** need to be declared in `vocabulary:`.
- **Drift Immunity**: Columns claimed by `column_groups` are treated as mapped, preventing false schema drift warnings.

### 3. Enterprise Controlled Vocabulary (`vocabulary:`)
The authorized enterprise data model is declared directly in the YAML:
```yaml
      vocabulary:
        - {name: order_id,    type: bigint}
        - {name: customer_id, type: string}
        - {name: amount,      type: double}
        - {name: currency,    type: string}
        - {name: event_ts,    type: timestamp}
        - {name: status,      type: string}
```
Any mapping rule in `columns:` attempting to introduce unknown columns or invalid types is halted immediately at check time before Spark launches. Note that `vocabulary:` is strictly optional—if omitted, `columns:` operates with standard schema mapping.

### 4. Partner Identification via Auto-Injected Path Variables (`h.partner`)
Because `path_pattern: "partners/(?P<partner>[^/]+)/"` defines the named group `partner`, StreamWright automatically projects `partner` directly as a first-class string column onto the Spark DataFrame. Downstream SQL steps can access `h.partner` directly without any repetitive `regexp_extract(_source_file, ...)` expressions:
```sql
SELECT
  order_id,
  row_id,
  partner AS partner_id,  -- Auto-injected from path_pattern!
  customer_id,
  amount,
  metrics,
  extra_attributes,
  ...
FROM harmonized_feed
```
The raw file path remains preserved on `_source_file` for complete physical data lineage and debugging.

### 5. Automated Schema Drift Detection (`_schema_report`)
When Partner Alpha adds a new, unmapped column (`promo_code`) to their file:
1. StreamWright detects the header signature change by comparing its SHA-256 hash against `sw_schema_signatures`.
2. Flags the file as `status = 'drift'` with unmapped columns listed in the internal `_schema_report` view.
3. **Output 3 (`schema_drift_audit_log`)** logs these events to PostgreSQL so data engineering teams receive automated alerts to update the schema registry—without crashing the pipeline!

### 6. Lazy File Row Numbering (`row_index: "row_id"`)
To trace individual records back to their line in the physical partner feed without triggering an expensive full-cluster shuffle:
- Adding `row_index: "row_id"` injects a lazy Catalyst window expression (`row_number().over(Window.partitionBy(input_file_name()).orderBy(monotonically_increasing_id())) - 1`).
- Generates a 0-indexed `row_id BIGINT` resetting per file.
- Clean rows carry `row_id` into Delta Lake, and quarantined corrupted rows preserve `row_id` for exact line-number auditing!

### 7. Zero-Overhead Row-Level Quality Routing (`row_errors: true`)
StreamWright introduces zero extra Spark actions (`count()`, `collect()`, and `show()` are strictly forbidden):
- Setting `row_errors: true` compiles a pure Catalyst column expression:
  ```sql
  _row_errors = array_compact(array(
    CASE WHEN try_cast(amount AS double) IS NULL THEN 'amount' END,
    CASE WHEN order_id IS NULL THEN 'order_id' END
  ))
  ```
- **Dual-Branch Routing**:
  - **Output 1 (Clean Data)**: `SELECT * FROM harmonized_orders WHERE size(_row_errors) = 0` merges directly into Delta Lake.
  - **Output 2 (Dead-Letter Quarantine)**: `SELECT * FROM harmonized_orders WHERE size(_row_errors) > 0` writes corrupted records (such as Alpha's invalid amount row) to an S3 quarantine bucket with human-readable `failure_reasons`.

### 8. Replay-Stable Plan Pinning
Every mapping decision, header signature, and file rejection is serialized into the batch manifest under `pinned["ingest:harmonized_feed"]`. On cluster retry or batch replay, StreamWright reconstructs the exact plan from the manifest without querying S3 or Hadoop FileSystem.

### 9. Declarative Environment Mapping (`runtime.env:`)
- Non-secret variables declared under `runtime.env` are mapped into:
  - `spark.yarn.appMasterEnv.<KEY>` for the YARN ApplicationMaster container.
  - `spark.executorEnv.<KEY>` for all distributed Spark executor task containers.
  - `os.environ[<KEY>]` for driver runs.
- `STREAMWRIGHT_SECRET_*` values stay on the driver and are never copied into Spark configuration.
- Avoids writing repetitive `--conf spark.yarn.appMasterEnv.X=...` and `--conf spark.executorEnv.X=...` flags.

### 10. Multi-Secret & Plaintext Resolution (`runtime.secrets:`)
- `mapping:` connects declared secret names (`audit_db_password`, `ops_db_password`) directly to individual AWS Secrets Manager IDs or ARNs.
- Handles raw plaintext strings (e.g. `supersecret`) as well as multi-key JSON objects (`{"password": "..."}`) in a single resolution step.

### 11. Resilient EMR Cluster Targeting (`runtime.platform.cluster.stack_name:`)
- Targets clusters dynamically by CloudFormation Stack Name or Stack ARN (`stack_name: "prod-data-platform-emr"`).
- Automatically discovers the live physical cluster ID (`j-XXXXXXXXXXXX`) when clusters are terminated and recreated by IaC pipelines.

---

## Specification Reference (`partner_orders.yaml`)

| Section | Role | Key Settings |
|---|---|---|
| `connections.landing_s3` | Raw Partner Drop-Zone | `s3://{{ config.landing_bucket }}/` |
| `connections.lake_delta` | Analytical Lakehouse Table | Inherits credentials from `lake_s3` |
| `connections.audit_db` | Operational Audit Log | PostgreSQL relational storage |
| `connections.ops_db` | StreamWright Control Plane | State store for leases & manifests |
| `steps.harmonized_feed` | Multi-Partner Reader | `format: auto`, `row_index: "row_id"`, embedded `columns:`, embedded `vocabulary:`, `row_errors: true` |
| `steps.harmonized_orders`| Transformation & Checkpoint | `checkpoint: true`, `persist: memory_and_disk` |
| `outputs.clean_orders_delta_merge` | Lakehouse Merge | `schema_evolution: add_columns`, `target_predicate:` |
| `outputs.quarantine_corrupted_records` | Dead-Letter Storage | `format: jsonl`, `layout: dt={today:%Y-%m-%d}` |
| `outputs.schema_drift_audit_log` | Drift Telemetry | `SELECT ... FROM _schema_report WHERE status = 'drift'` |
| `outputs.file_rejection_audit_log` | File Errors Telemetry | `SELECT ... FROM _file_errors` |

---

## Validating and Running the Pipeline

### 1. Validate Job Contract & SQL Syntax
```bash
python -m streamwright.spark.cli.main validate examples/s3/partner_orders.yaml
```
Output:
```text
examples/s3/partner_orders.yaml: ok
```

### 2. Seed Test Partner Feeds & PostgreSQL Audit Tables
Run the dedicated seeding script to provision the test files in LocalStack S3 and create the Postgres audit schema:
```bash
./docker/seed_partner_orders.sh
```

### 3. Option A: Run Locally on Host (macOS / Linux)

Run the pipeline directly with your local Python / PySpark environment using `partner_orders.local.yaml`:

```bash
# Set secrets and LocalStack endpoint via environment variables
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export STREAMWRIGHT_SECRET_OPS_DB_PASSWORD="change-me-locally"
export STREAMWRIGHT_SECRET_AUDIT_DB_PASSWORD="change-me-locally"
export AWS_ENDPOINT_URL="http://127.0.0.1:4566"

# 1. Validate job contract & expressions with local overlay
sw spark validate examples/s3/partner_orders.yaml \
  --overlay examples/s3/partner_orders.local.yaml \
  --strict

# 2. Run a single microbatch batch (--once with --summary)
sw spark run examples/s3/partner_orders.yaml \
  --overlay examples/s3/partner_orders.local.yaml \
  --set landing_bucket=acme-landing \
  --set lake_bucket=acme-lake \
  --once --summary
```

---

### 4. Option B: Run Inside Docker Container (Blind spark-submit)

Run the pre-configured runner script (or spark-submit command):
```bash
./docker/submit_partner_orders.sh
```
Or directly via `docker run`:
```bash
docker run --rm \
  --network streamwright-spark_default \
  -e AWS_ACCESS_KEY_ID=test \
  -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e AWS_ENDPOINT_URL=http://localstack:4566 \
  -e STREAMWRIGHT_SECRET_OPS_DB_PASSWORD=change-me-locally \
  -e STREAMWRIGHT_SECRET_AUDIT_DB_PASSWORD=change-me-locally \
  spark-clean:latest \
  --master "local[2]" \
  --conf spark.hadoop.fs.s3a.endpoint="http://localstack:4566" \
  --conf spark.hadoop.fs.s3a.path.style.access="true" \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled="false" \
  --conf spark.hadoop.fs.s3a.impl="org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf spark.sql.extensions="io.delta.sql.DeltaSparkSessionExtension" \
  --conf spark.sql.catalog.spark_catalog="org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf spark.sql.catalogImplementation="hive" \
  --conf spark.hadoop.hive.metastore.uris="thrift://metastore:9083" \
  --py-files s3a://code-bucket/packages/streamwright-0.1.8-py3-none-any.whl,s3a://code-bucket/packages/streamwright_spark-0.0.0-py3-none-any.whl,s3a://code-bucket/packages/streamwright_s3-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_postgres-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_deltalake-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_kafka-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_sqs-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_files-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_gcs-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_azure_blob-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/delta_spark-3.2.1-py3-none-any.whl \
  s3a://code-bucket/entrypoint.py \
  --job s3://code-bucket/jobs/s3/partner_orders.yaml \
  --overlay s3://code-bucket/jobs/s3/partner_orders.docker.yaml \
  --set landing_bucket=acme-landing \
  --set lake_bucket=acme-lake \
  --once --summary
```
Output:
```text
Batch s3_partner_orders-00000001 summary: 2 read, 1 rejected, 1 drifted, 2 distinct signatures
```

---

### 5. Verifying Output Data

#### A. Trino SQL (Harmonized Delta Table)
```bash
docker compose exec trino trino --execute \
  "SELECT order_id, row_id, partner_id, customer_id, amount, currency, metrics, extra_attributes FROM delta.analytics.harmonized_orders ORDER BY order_id;"
```

#### B. PostgreSQL Audit Tables (`audit.schema_drift_events` & `audit.file_rejection_events`)
```bash
docker compose exec postgres psql -U sw -d streamwright -P pager=off -c \
  "SELECT step, file_uri, status, header_hash, unmapped_columns FROM audit.schema_drift_events;"

docker compose exec postgres psql -U sw -d streamwright -P pager=off -c \
  "SELECT step, file_uri, reason FROM audit.file_rejection_events;"
```

#### C. LocalStack S3 Dead-Letter Quarantine (`s3://acme-lake/quarantine/orders/`)
```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://127.0.0.1:4566 s3 ls s3://acme-lake/quarantine/orders/ --recursive
```
