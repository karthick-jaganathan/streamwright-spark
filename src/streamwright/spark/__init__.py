"""
StreamWright Spark (`streamwright-spark`): Distributed Batch Processing Engine for StreamWright.

StreamWright Spark executes declarative batch data pipelines on Apache Spark (EMR EC2, EMR Serverless,
Dataproc, Kubernetes, or local clusters). It pairs distributed Spark execution on the data plane
(writing directly to S3, GCS, Azure Blob, and Delta Lake) with transactional bookkeeping on the
control plane (PostgreSQL or SQLite).

---

## 1. Pipeline Architecture & Execution Model

A `kind: SparkJob` pipeline follows a deterministic two-phase lifecycle:
1. **Driver-Side Coordination**:
   - Acquires an exclusive, epoch-fenced distributed lease in `sw_leases` (preventing split-brain).
   - Polls input events/messages (e.g. from Kafka or SQS) and registers the pinned manifest in `sw_batches`.
   - Resolves configuration tokens (`{{ config.X }}`), secrets (`{{ secrets.Y }}`), and batch references (`:batch_id`).
2. **Worker-Side Distributed Execution**:
   - Steps execute lazily as Spark temporary views (`createOrReplaceTempView`), compiling into a DAG.
   - Outputs execute as eager Spark actions (`.save()`, `.jdbc()`, or `DeltaTable.merge()`).
   - Sinks record durable write receipts in `sw_receipts` upon commit.
   - Downstream completion events are recorded in `sw_outbox` and relayed only after storage is durable.
   - Broker consumer offsets are acknowledged (`consumer.ack()`) **only after** all outputs and outbox entries succeed.

---

## 2. Core Rules & Design Invariants

- **Lazy Steps vs. Eager Outputs**:
  - `steps:` declare SQL views, remote table scans, and event consumption. They never write to external storage.
  - `outputs:` declare external writes. Every output step requires `sql:` and triggers an atomic commit.
- **Idempotency & Replay Protection**:
  - If a batch fails midway (e.g. on output 3 of 4), outputs 1 and 2 record write receipts in `sw_receipts`.
  - Upon retry, the runner skips completed outputs, preventing duplicate writes.
- **Caching & Lineage Control**:
  - `persist: StorageLevel`: Caches intermediate DataFrames using any standard PySpark `StorageLevel` (e.g.,
    `MEMORY_AND_DISK`, `MEMORY_ONLY`, `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `OFF_HEAP`).
  - `checkpoint: true`: Truncates unbounded Spark DAG lineage graphs to physical storage (`spark.checkpoint.dir`),
    preventing Out-Of-Memory (OOM) and StackOverflow errors during recursive or iterative operations.
- **Dynamic Partition Pruning (`target_predicate`)**:
  - Delta merges accept a source-derived `target_predicate` (e.g. `target.event_date IN ({{ step.event_date }})`).
  - Catalyst limits Delta's file scan strictly to matching partition directories, cutting >95% of S3 I/O.
- **Fine-Grained Column Mapping (`set` in `when_matched`)**:
  - `when_matched` supports explicit column-level dictionary mapping (`set: {col: source.col}`) to update only
    intended fields while preserving immutable original audit attributes.
  - Omitting `set` invokes Delta's unconditional `whenMatchedUpdateAll()`.
- **Declarative Catalog Registration & 0-Overhead Caching**:
  - Writes can register tables into Hive Metastore or DeltaCatalog automatically via `table_name`, `schema`,
    `schema_location`, `schema_comment`, `table_properties`, and `comment`.
  - The Spark driver caches registration in memory: DDL runs once on Batch 1, adding 0 ms on subsequent batches.
- **Atomic Commit Receipts (`_commits` view)**:
  - Every Delta write/merge injects an atomic commit receipt into the internal `_commits` temporary view
    (`output`, `table_name`, `version`, `rows`, `batch_id`).
  - Downstream steps and Kafka outbox publishers read exact commit versions with zero extra reads against S3.
- **Durable Broker Acknowledgement**:
  - `enable.auto.commit` is strictly disabled for Kafka.
  - Consumer offsets are committed synchronously (`asynchronous=False`) only after all business outputs and the
    outbox are committed to durable storage.
- **Storage Credentials Inheritance**:
  - Analytical storage tables (such as `deltalake`) inherit their underlying filesystem credentials from the
    storage connection specified in `storage:` (e.g. `storage: landing_s3`).

---

## 3. Top-Level `kind: SparkJob` Specification

```yaml
kind: SparkJob
version: 1
name: s3_events_to_delta
description: Consumes S3 arrival notifications, audits state, merges Delta tables, exports client archives, and emits outbox events.

spec:
  config:                               # Typed configuration parameters
    lake_bucket: {type: string}
    kafka_bootstrap: {type: string}
  secrets:                              # Declared secret keys
    pg_password: {type: string}

connections:                            # Storage, database, and broker connections
  landing_s3:
    provider: s3
    roots: ["s3://my-landing-bucket/"]
    region: us-east-1

  lake_delta:
    provider: deltalake
    roots: ["s3://{{ config.lake_bucket }}/"]
    storage: landing_s3
    schema: analytics                   # Default catalog schema
    schema_location: "s3://{{ config.lake_bucket }}/analytics"
    schema_comment: "Central enterprise analytics data mart"

  kafka_bus:
    provider: kafka
    bootstrap_servers: "{{ config.kafka_bootstrap }}"
    security_protocol: SASL_SSL
    sasl_mechanism: AWS_MSK_IAM
    region: us-east-1

  ops_db:
    provider: postgres
    host: ops.internal.net
    database: ops
    user: sw_state
    password: "{{ secrets.pg_password }}"

steps:                                  # DAG views (transformations, scans, consume)
  - name: arrivals
    connection: kafka_bus
    service: topic
    method: consume
    arguments:
      topic: "lake.file_arrived.v1"
      group.id: "streamwright-ingest"
      auto.offset.reset: earliest
      max.poll.records: 500
      max_wait: 30s
    sql: "SELECT message_id, from_json(body, 'file_uri STRING, client_id STRING') AS payload FROM input"

  - name: raw_orders
    connection: landing_s3
    service: object
    method: read
    arguments:
      path: "{{ arrivals.payload.file_uri }}"
      format: csv
      options:
        header: true

  - name: processed_orders
    persist: memory_and_disk
    checkpoint: true                    # Sever recursive Catalyst lineage before fan-out
    sql: |
      SELECT client_id, order_id, item_name, amount, tax, total_amount,
             event_ts, CAST(event_ts AS DATE) AS event_date,
             input_file_name() AS source_file, :batch_id AS batch_id
      FROM raw_orders

outputs:                                # Distributed writes, upserts, and publish
  # Output 1: Two-Phase Bookkeeping Init (Zero Heavy I/O)
  - name: init_processing
    connection: ops_db
    service: database
    method: append
    arguments:
      table: ingestion_audit
    sql: |
      SELECT :batch_id AS batch_id, 'PROCESSING' AS status, current_timestamp() AS updated_at

  # Output 2: Delta Lake Merge with Dynamic Partition Pruning & Column Set Mapping
  - name: orders_delta
    connection: lake_delta
    service: tables
    method: merge
    arguments:
      table_path: "s3://{{ config.lake_bucket }}/delta/orders"
      table_name: orders
      schema: analytics
      table_properties:
        "delta.autoOptimize.optimizeWrite": "true"
        "delta.autoOptimize.autoCompact": "true"
      comment: "Central enterprise orders table"
      partition_by: [client_id, event_date]
      condition: "target.order_id = source.order_id AND target.client_id = source.client_id"
      target_predicate: "target.event_date IN ({{ processed_orders.event_date }})"
      when_matched:
        - action: update
          set:
            item_name: source.item_name
            amount: source.amount
            tax: source.tax
            total_amount: source.total_amount
            event_ts: source.event_ts
            batch_id: source.batch_id
      when_not_matched:
        - action: insert
    sql: "SELECT * FROM processed_orders"

  # Output 3: Multi-Client Partitioned Split CSV Exports
  - name: export_client_archives
    connection: landing_s3
    service: object
    method: write
    arguments:
      path: "s3://{{ config.lake_bucket }}/exports/orders"
      format: csv
      mode: overwrite
      split_by: [client_id, event_date]
      layout: "{{ client_id }}/{{ event_date }}"
      file_name: "orders_{{ client_id }}_{{ event_date }}.csv.gz"
      options:
        header: true
        compression: gzip
    sql: "SELECT * FROM processed_orders"

  # Output 4: Two-Phase Bookkeeping Completion (Zero Redundant Reads)
  - name: mark_completed
    connection: ops_db
    service: database
    method: append
    arguments:
      table: ingestion_audit
    sql: |
      SELECT :batch_id AS batch_id, 'COMPLETED' AS status, version AS delta_version,
             rows AS rows_committed, current_timestamp() AS updated_at
      FROM _commits
      WHERE output = 'orders_delta'

  # Output 5: Downstream Outbox Notification
  - name: announce_done
    connection: kafka_bus
    service: topic
    method: publish
    arguments:
      topic: "lake.orders.committed.v1"
      key: table_name
    sql: "SELECT table_name, version, rows, batch_id FROM _commits WHERE output = 'orders_delta'"

runtime:
  state: {connection: ops_db, schema: sw_ops}   # Or {sqlite: "/data/ops.sqlite"} for local/dev
  spark:
    version: "3.5.x"
    conf:
      spark.checkpoint.dir: "s3://{{ config.lake_bucket }}/checkpoints/"
      spark.executor.memory: 4g
  platform:
    type: emr_ec2                               # local | emr_ec2 | emr_serverless | dataproc
    region: us-east-1
```

---

## 4. Connector Method Specifications

### Apache Kafka (`provider: kafka`)
- `topic.consume`:
  - `topic` (string, required): Kafka topic name.
  - `group.id` (string): Standard Kafka consumer group ID (or `group_id`).
  - `auto.offset.reset` (string, default `earliest`): Starting offset policy (`earliest` | `latest` | `error`).
  - `max.poll.records` (integer, default 500): Maximum records per batch poll (or `max_messages`).
  - `max_wait` (duration, default `30s`): Polling deadline (or `fetch.max.wait.ms`).
  - `options` (object): Arbitrary vendor client options passed to `confluent_kafka.Consumer(conf)`.
- `topic.publish`:
  - `topic` (string, required): Target topic name.
  - `key` (string): Column name used as partition message key.
  - `options` (object): Arbitrary vendor client options passed to `confluent_kafka.Producer(conf)`.

### Amazon SQS (`provider: sqs`)
- `queue.consume`:
  - `QueueUrl` (string, required): SQS queue URL (or `queue_url`).
  - `MaxNumberOfMessages` (integer, default 100): Batch size (or `max_messages`).
  - `WaitTimeSeconds` (integer): Long-polling wait seconds (or `max_wait`).
  - `VisibilityTimeout` (integer): Visibility timeout seconds (or `visibility_timeout`).
  - `options` (object): Extra kwargs passed to `boto3.client('sqs').receive_message`.
- `queue.publish`:
  - `QueueUrl` (string, required): Target SQS queue URL.
  - `MessageGroupId` (string): FIFO message group ID (or `group_id`).
  - `options` (object): Extra kwargs passed to `boto3.client('sqs').send_message_batch`.

### Delta Lake (`provider: deltalake`)
- Connection properties:
  - `storage` (string): Inherits S3/GCS/Azure filesystem credentials from another declared connection.
  - `roots` (list[string]): Storage roots permitted for Delta tables.
  - `schema` (string): Default catalog schema / database.
  - `schema_location` (string): Storage directory for the catalog schema.
  - `schema_comment` (string): Schema-level documentation.
- `tables.scan`:
  - `table_path` (string, required): Table path or URI.
  - `columns`, `where`, `limit`, `version` (integer, time travel), `options` (e.g. `readChangeFeed: "true"`).
- `tables.append`:
  - `table_path` (string, required), `partition_by`, `options` (e.g. `mergeSchema: "true"`).
  - Declarative catalog registration: `table_name`, `schema`, `schema_location`, `schema_comment`, `table_properties`, `comment`.
- `tables.overwrite`:
  - `table_path` (string, required), `partition_by`, `options`.
  - `replaceWhere` (string predicate, or `replace_where`), `whole_table` (bool).
  - Declarative catalog registration: `table_name`, `schema`, `schema_location`, `schema_comment`, `table_properties`, `comment`.
- `tables.merge`:
  - `table_path` (string, required), `condition` (string ON clause).
  - `target_predicate` (string): Source-derived partition pruning predicate for Delta file skipping.
  - `when_matched` (list[object]):
    - `action: update`: Optional `condition`, and optional `set: {target_col: source_expr}` dictionary mapping (omitting `set` invokes `whenMatchedUpdateAll`).
    - `action: delete`: Optional `condition`.
  - `when_not_matched` (list[object]):
    - `action: insert`: Optional `condition`, and optional `values: {target_col: source_expr}` dictionary mapping (omitting `values` invokes `whenNotMatchedInsertAll`).
  - `when_not_matched_by_source` (list[object]):
    - `action: update` (with `set: {...}`) or `action: delete`.
  - Declarative catalog registration: `table_name`, `schema`, `schema_location`, `schema_comment`, `table_properties`, `comment`.
  - 0-overhead in-memory driver caching: Metastore DDL runs once on Batch 1, adding 0 ms on subsequent batches.
  - Atomic commit receipts: Emits table name, version, rows, and timestamp into `_commits` view.

### Object Stores & Filesystems (`provider: s3 | gcs | azure_blob | files`)
- `object.read` / `file.read`:
  - `path` (string, required): Path string or dynamic reference `{{ step.column }}`.
  - `format` (string, default `auto`): `csv`, `tsv`, `json`, `jsonl`, `parquet`.
  - `options` (object): Unmasked PySpark reader options (`header`, `inferSchema`, `sep`, `delimiter`, `multiLine`, `dateFormat`, etc.).
  - `columns` / `vocabulary`: Per-client header mapping and schema harmonization.
- `object.write` / `file.write`:
  - `path` (string, required): Target destination path.
  - `format` (string, required): `csv`, `tsv`, `json`, `jsonl`, `parquet`.
  - `mode` (string, default `error`): `overwrite`, `append`, `error`, `ignore`.
  - `options` (object): Unmasked PySpark writer options (`compression`, `quoteAll`, etc.).
  - Split exports: Partition writes into distinct client files via `split_by: [col1, col2]`, `layout: "{{ col1 }}/{{ col2 }}"`, and `file_name: "pattern_{{ col1 }}.csv.gz"`.

### PostgreSQL / JDBC (`provider: postgres`)
- `database.query`: `query` (required SQL), `params` (dict), `options` (vendor JDBC properties like `fetchsize`, `queryTimeout`).
- `database.table`: `table` (required), `schema`, `columns`, `where`, `options`.
- `database.append`: `table` (required), `options`. Used for two-phase audit logging (`PROCESSING` -> `COMPLETED`).
- `database.upsert`: `table` (required), `key` (list), `update` (list), `staging_table` (required), `options`.
- `database.delete`: `table` (required), `key` (list), `staging_table` (required), `options`.

---

## 5. CLI Commands Reference

- `sw spark validate PATH [--overlay FILE] [--analyze] [--graph] [--strict]`:
  Statically validates pipeline structure, connections, SQL references, and vendor arguments.
- `sw spark run PATH [--overlay FILE] [--once | --drain | --follow] [--log-level LEVEL] [--conf K=V] [--set K=V]`:
  Submits or executes the pipeline batch loop (supports log levels: ERROR, WARN, INFO, DEBUG).
- `sw spark lock PATH [--overlay FILE]`:
  Resolves and locks all dependencies, Maven coordinates, and connector versions.
- `sw spark state (status | retry | abandon | init) PATH`:
  Inspects or manages distributed leases, failed batches, and state store tables.
"""

from streamwright.spark.contracts.types import CONTRACT_VERSION

__all__ = ["CONTRACT_VERSION"]
