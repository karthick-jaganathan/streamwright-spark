# S3 File Events to Delta Lake Ingestion Pipeline

An end-to-end, production-grade batch ingestion pipeline executed on **StreamWright Spark**:

```
[Kafka Bus: lake.s3_file_events.v1] (S3 ObjectCreated notifications)
                   │
                   ▼
[1. file_arrivals] ──► [2. parsed_file_events] ──► [3. distinct_files] (Cached)
                                                           │
             ┌─────────────────────────────────────────────┼─────────────────────────────────────────────┐
             ▼                                             ▼                                             ▼
  [OUTPUT 1: Bookkeeping Init]               [4. file_contents (Amazon S3)]                 [OUTPUT 4: Bookkeeping Done]
  - Target: Postgres file_event_audit                      │                                - Target: Postgres file_event_audit
  - Status: 'PROCESSING'                                   ▼                                - Status: 'COMPLETED'
  - I/O: Only evaluates Kafka metadata       [5. processed_orders] (checkpoint: true)       - Metrics: rows_written, delta_version
  - S3 files NOT touched!                                  │                                - Source: _commits + cached DataFrame
                                                           │                                - ZERO re-reading of S3 or Delta!
                               ┌───────────────────────────┴───────────────────────────┐
                               ▼                                                       ▼
                [OUTPUT 2: Delta Lake MERGE]                            [OUTPUT 3: Split File Exports]
                - Target: Delta Lake partitioned table                  - Target: Amazon S3 (gzip CSVs)
                - Target Predicate File Pruning                         - Partitioned by {client_id}/dt={event_date}
                - Condition: order_id & client_id match                 - File: {client_id}_{event_date}_orders.csv.gz
                - Generates atomic commit receipt in _commits
                               │
                               ▼ Commit Receipt (_commits)
                [OUTPUT 5: Downstream Outbox Announcement]
                - Target: Kafka (lake.orders.committed.v1)
                - Emits Delta commit version, row count, and batch_id
```

---

## Architectural Principles & Invariants

### 1. Two-Phase Operational Bookkeeping (Zero Heavy I/O in Step 1)
- **Problem**: If an executor crashes while downloading large files from S3, how do operators know which files were being processed?
- **Solution**: Output 1 (`bookkeeping_files_received`) executes **first**, writing `status = 'PROCESSING'` into PostgreSQL.
- **Spark Optimization**: Because Spark evaluates execution DAGs lazily, Output 1 only triggers evaluation of `distinct_files` (the Kafka event metadata). **Step 4 (`file_contents`) is completely bypassed**, so no heavy S3 files are downloaded or parsed during this phase.

### 2. PySpark Lineage Checkpointing (`checkpoint: true`)
- Step 5 (`processed_orders`) sets `checkpoint: true`.
- PySpark writes intermediate partitions to `spark.checkpoint.dir` on S3 or local disk, severing memory references to upstream Catalyst DAGs.
- This prevents JVM `StackOverflowError` and driver memory bloat when downstream outputs fan out to multiple tables and exports.

### 3. Delta Lake MERGE with Target Predicate Pruning
- Merging data into large historical partitioned Delta tables often results in full-table scans.
- By configuring:
  ```yaml
  target_predicate: "target.event_date IN ({{ processed_orders.event_date }})"
  ```
  Streamwright inspects the distinct `event_date` values in the current batch and applies partition filters directly to Delta's file scanner, eliminating >95% of S3 read I/O.

### 4. High-Performance Multi-Client Split Exports
- Output 3 partitions records using:
  ```yaml
  split_by: [client_id, event_date]
  layout: "{client_id}/dt={event_date:%Y-%m-%d}"
  file_name: "{client_id}_{event_date:%Y%m%d}_orders_{today}.csv.gz"
  ```
  This generates organized, compressed client archives in S3 in a single pass directly from the cached DataFrame.

### 5. Completion Bookkeeping with Virtual Commit Receipts (`_commits`)
- Output 4 (`bookkeeping_files_completed`) updates audit records to `status = 'COMPLETED'`.
- It joins with the virtual view `_commits WHERE output = 'delta_orders_merge'` to record the exact Delta ACID commit version and rows written.
- **Zero redundant reads**: Neither S3 files nor the Delta table are queried again.

### 6. Downstream Triggering via Outbox
- Output 5 publishes a commit event to Kafka topic `lake.orders.committed.v1` containing the Delta version and row counts.
- Downstream consumer DAGs (e.g. Silver/Gold transformations) consume this event to trigger their workflows.

### 7. Guaranteed Broker Acknowledgement
- Streamwright disables Kafka auto-commit (`enable.auto.commit: false`).
- Consumer offsets are committed synchronously to the Kafka broker **only after all 5 outputs succeed**. If any failure occurs, offsets remain uncommitted, ensuring safe replay.

---

## Manual Execution Guide

### 1. Prerequisites & Background Services

Start the supporting infrastructure (Kafka, LocalStack S3, PostgreSQL) in Docker:
```bash
# In repos/streamwright-spark:
docker compose up -d postgres localstack kafka
```

Seed initial data (creates buckets, tables, uploads sample landing files, and produces S3 notification events to Kafka):
```bash
make seed
```

---

### 2. Option A: Run Locally on Host (macOS / Linux)

Run the pipeline directly with your local Python / PySpark environment using `overlays/local.yaml`:

```bash
# Set secrets via environment variables
export STREAMWRIGHT_SECRET_OPS_DB_PASSWORD="change-me-locally"
export STREAMWRIGHT_SECRET_REPORTING_DB_PASSWORD="change-me-locally"
export STREAMWRIGHT_SECRET_AWS_ACCESS_KEY_ID="localstack"
export STREAMWRIGHT_SECRET_AWS_SECRET_ACCESS_KEY="localstack-secret"

# 1. Validate the job definition with the local overlay
sw spark validate examples/s3_events_to_delta \
  --overlay overlays/local.yaml \
  --strict \
  --analyze

# 2. Run a single batch (--once, with clean logs via --log-level ERROR)
sw spark run examples/s3_events_to_delta \
  --overlay overlays/local.yaml \
  --set kafka_bootstrap=127.0.0.1:9094 \
  --set lake_bucket=acme-lake \
  --log-level ERROR \
  --once

# 3. Or run continuously in streaming follow mode (--follow)
sw spark run examples/s3_events_to_delta \
  --overlay overlays/local.yaml \
  --set kafka_bootstrap=127.0.0.1:9094 \
  --set lake_bucket=acme-lake \
  --log-level WARN \
  --follow \
  --interval 10s
```

---

### 3. Option B: Run Inside Docker Container

Run inside the bundled `sw` container using `overlays/docker.yaml`:

```bash
# Build the container image
docker compose build sw

# 1. Validate inside container
docker compose run --rm sw validate /work/examples/s3_events_to_delta \
  --overlay /work/overlays/docker.yaml \
  --strict

# 2. Run a batch inside container
docker compose run --rm sw run /work/examples/s3_events_to_delta \
  --overlay /work/overlays/docker.yaml \
  --once
```

---

### 4. Inspecting Pipeline State & Bookkeeping

Inspect batch progress, view audit receipts, or retry failed batches:

```bash
# List recent batches
sw spark state batches examples/s3_events_to_delta \
  --overlay overlays/local.yaml \
  --set kafka_bootstrap=127.0.0.1:9094 \
  --set lake_bucket=acme-lake

# View detailed batch manifest and write receipts
sw spark state show examples/s3_events_to_delta s3_events_to_delta-00000001 \
  --overlay overlays/local.yaml \
  --set kafka_bootstrap=127.0.0.1:9094 \
  --set lake_bucket=acme-lake

# Queue a failed or interrupted batch for retry
sw spark state retry examples/s3_events_to_delta s3_events_to_delta-00000001 \
  --overlay overlays/local.yaml \
  --set kafka_bootstrap=127.0.0.1:9094 \
  --set lake_bucket=acme-lake
```

#### Directly Querying the `sw_*` State Tables in PostgreSQL

StreamWright tracks its control-plane state under the **`sw_ops`** schema. You can query it via `docker compose exec`:

```bash
# 1. Batch lifecycle & status (sw_ops.sw_batches)
docker compose exec postgres psql -U sw -d streamwright -P pager=off -c \
  "SELECT batch_id, seq, status, created_at, updated_at FROM sw_ops.sw_batches WHERE job = 's3_events_to_delta' ORDER BY seq;"

# 2. Output write receipts (sw_ops.sw_receipts)
docker compose exec postgres psql -U sw -d streamwright -P pager=off -c \
  "SELECT batch_id, output, receipt FROM sw_ops.sw_receipts WHERE job = 's3_events_to_delta';"

# 3. Outbox broker events (sw_ops.sw_outbox)
docker compose exec postgres psql -U sw -d streamwright -P pager=off -c \
  "SELECT batch_id, output, topic, status, payload FROM sw_ops.sw_outbox WHERE job = 's3_events_to_delta';"

# 4. Distributed leases & epochs (sw_ops.sw_leases)
docker compose exec postgres psql -U sw -d streamwright -P pager=off -c \
  "SELECT job, owner, epoch, expires_at FROM sw_ops.sw_leases WHERE job = 's3_events_to_delta';"
```

---

### 5. Verifying Output Data

#### A. PostgreSQL Operational Audit Table (`reporting.file_event_audit`)
```bash
python -c "
import psycopg
with psycopg.connect(host='127.0.0.1', port=5432, user='sw', password='change-me-locally', dbname='streamwright') as conn:
    with conn.cursor() as cur:
        cur.execute('SELECT file_uri, client_id, status, rows_written, delta_version, completed_at FROM reporting.file_event_audit;')
        for row in cur.fetchall():
            print(row)
"
```

#### B. S3 Split Client Exports
```bash
AWS_ACCESS_KEY_ID=localstack AWS_SECRET_ACCESS_KEY=localstack-secret \
  aws --endpoint-url=http://127.0.0.1:4566 s3 ls s3://acme-lake/exports/orders/ --recursive
```

#### C. S3 Delta Lake Table
```bash
AWS_ACCESS_KEY_ID=localstack AWS_SECRET_ACCESS_KEY=localstack-secret \
  aws --endpoint-url=http://127.0.0.1:4566 s3 ls s3://acme-lake/delta/orders/ --recursive
```

#### D. Kafka Outbox Announcement Event (`lake.orders.committed.v1`)
```bash
python -c "
import confluent_kafka as kafka
c = kafka.Consumer({'bootstrap.servers': '127.0.0.1:9094', 'group.id': 'verifier', 'auto.offset.reset': 'earliest'})
c.subscribe(['lake.orders.committed.v1'])
msg = c.poll(5.0)
if msg:
    print('Key:', msg.key())
    print('Payload:', msg.value().decode())
c.close()
"
```

