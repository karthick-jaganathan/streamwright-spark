# StreamWright Spark Cookbook

Practical, production-tested recipes, operational guides, and architectural patterns for building and operating distributed event-driven Spark pipelines with StreamWright.

---

## Cookbooks Index

### 1. [Local Development, Cluster Lifecycle & Testing](01_local_development_and_testing.md)
Step-by-step guide to managing the local multi-service cluster:
- Clean volume purge & teardown (`make down`).
- Starting PostgreSQL, LocalStack S3, Kafka, Hive Metastore, and Trino (`make lakehouse`).
- Automated topic and data seeding (`make seed-order-files`).
- Packaging and syncing artifacts to S3 (`make publish-s3`).
- Running jobs via blind container submission (`make submit-order-files`) or direct host execution (`sw spark run`).
- Querying and inspecting Delta tables in Trino (`make trino`).

### 2. [Delta Lake Merge & Declarative Catalog Registration](02_delta_lake_merge_and_catalog.md)
Deep dive into ACID lakehouse operations:
- Event-driven Delta Lake `merge` with source-derived partition pruning (`target_predicate`).
- Explicit column-level `set` mapping under `when_matched` vs `updateAll`.
- Declarative catalog registration in Hive Metastore: custom `schema`, `schema_location`, `schema_comment`, table `comment`, and persistent `table_properties` (`TBLPROPERTIES`).
- Zero-overhead in-memory driver caching across micro-batches 2..N.

### 3. [Production Deployment & S3 Artifact Serving](03_production_deployment_s3_artifacts.md)
Architectural pattern for running blind Spark jobs on cloud clusters (EMR, Kubernetes, Databricks):
- Zero-dependency worker nodes (no pre-installed StreamWright on images).
- Dynamic runtime wheel unpacking via `entrypoint.py` and `--py-files`.
- Decoupling infrastructure images from pipeline application versions.

### 4. [Multi-Schema Microbatch Ingestion & Quality Routing](04_multi_schema_microbatch_processing.md)
End-to-end guide to multi-schema microbatch handling:
- Preventing native Spark positional column misalignment via `schema_mode: by_name`.
- Per-file format resolution (`format: auto`) for mixed batches.
- Replay-stable plan pinning in `manifest["pinned"]["ingest:<step>"]`.
- Schema drift visibility via `_schema_report` and `on_unmapped_column`.
- Row-level quality routing (`row_errors: true` & `_row_errors`).
- Delta Lake schema evolution (`schema_evolution: strict | add_columns`).

---

## Canonical Pipeline Examples
- [**`examples/kafka/order_files.yaml`**](../examples/kafka/README.md): Event-driven Kafka notifications, PostgreSQL two-phase operational bookkeeping, Delta Lake merge with target predicate partition pruning, column groups, and split file exports.
- [**`examples/s3/partner_orders.yaml`**](../examples/s3/README.md): Heterogeneous multi-partner feeds, dynamic URI prefix mapping, enterprise controlled vocabulary, schema drift auditing (`_schema_report`), zero-overhead row error dead-letter quarantine, and Delta Lake merge.
