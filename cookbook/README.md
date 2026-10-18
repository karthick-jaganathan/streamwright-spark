# StreamWright Spark Cookbook

Practical, production-tested recipes, operational guides, and architectural patterns for building and operating distributed event-driven Spark pipelines with StreamWright.

---

## Cookbooks Index

### 1. [Local Development, Cluster Lifecycle & Testing](01_local_development_and_testing.md)
Step-by-step guide to managing the local multi-service cluster:
- Clean volume purge & teardown (`make down`).
- Starting PostgreSQL, LocalStack S3, Kafka, Hive Metastore, and Trino (`make lakehouse`).
- Automated topic and data seeding (`make seed`).
- Packaging and syncing artifacts to S3 (`make publish-s3`).
- Running jobs via blind container submission (`make submit-blind`) or direct host execution (`sw spark run`).
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

---

## Canonical Pipeline Example
For a fully working end-to-end pipeline implementation, see [`examples/s3_events_to_delta`](../examples/s3_events_to_delta/README.md).
