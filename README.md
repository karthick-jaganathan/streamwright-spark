# streamwright-spark

[![PyPI version](https://img.shields.io/pypi/v/streamwright-spark.svg)](https://pypi.org/project/streamwright-spark/)
[![Python versions](https://img.shields.io/pypi/pyversions/streamwright-spark.svg)](https://pypi.org/project/streamwright-spark/)
[![Documentation](https://img.shields.io/badge/docs-streamwright.web.app-brightgreen.svg)](https://streamwright.web.app/docs/)

**Distributed Batch Processing Engine on Apache Spark for StreamWright (`kind: SparkJob`).**

StreamWright Spark executes declarative batch data pipelines on Apache Spark. It pairs distributed execution on the data plane (object storage, lakehouses, databases) with transactional bookkeeping on the control plane (PostgreSQL, SQLite).

---

## Key Features

- **Declarative Batch Dataflows**: Author distributed Spark pipelines using structured YAML specifications (`kind: SparkJob`) with automatic DAG resolution and zero boilerplate driver code.
- **Sub-Second Offline Validation**: Validate pipeline configurations, check schema types, and analyze Catalyst query execution plans locally in pure Python—without starting a JVM or PySpark runtime.
- **Extensible Connector Architecture**: Decoupled reader and writer adapters for streaming brokers (Kafka, SQS), object stores (S3, GCS, Azure Blob), ACID lakehouses (Delta Lake), and relational databases (PostgreSQL/JDBC).
- **ACID Lakehouse Operations**: Transactional appends, overwrites, and partition-pruned upserts (`MERGE INTO`) with automated catalog schema/table registration.
- **Durable Bookkeeping & Recovery**: Epoch-fenced distributed leasing, step-level idempotency receipts, and automatic crash recovery across batch retries.
- **Flexible Cluster Deployments**: Run locally in-process, on standalone Spark clusters, or submit to cloud platforms (AWS EMR on EC2, EMR Serverless, Amazon EKS, Dataproc, Databricks).

---

## Installation

### Core Engine & CLI (Pure Python)
For static validation, DAG planning, and version locking (zero JVM or PySpark installation required):

```bash
pip install streamwright-spark
```

### With PySpark & Delta Lake
To execute pipelines with in-process Spark or local Delta Lake writes:

```bash
pip install "streamwright-spark[spark,delta]"
```

### Full Bundle
Includes PostgreSQL state store, Apache Kafka, Amazon SQS, and AWS authentication:

```bash
pip install "streamwright-spark[all]"
```

---

## CLI Commands

StreamWright Spark attaches directly to the unified `sw` / `streamwright` CLI:

| Command                                                              | Description                                                                                    |
|----------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `sw spark validate PATH [--overlay FILE] [--analyze] [--strict]`     | Statically validates pipeline structure, DAG dependencies, and vendor arguments without Spark. |
| `sw spark run PATH [--overlay FILE] [--once \| --drain \| --follow]` | Executes the pipeline batch loop (in-process local, cluster, or client mode).                  |
| `sw spark lock PATH [--overlay FILE]`                                | Resolves and pins all connector versions, dependencies, and Maven coordinates.                 |
| `sw spark state (status \| retry \| abandon \| init) PATH`           | Inspects and manages distributed leases, failed batches, and state store tables.               |

---

## Documentation & Cookbooks

For detailed operational guides, recipes, and architecture patterns:

- **[Cookbooks](https://github.com/karthick-jaganathan/streamwright-spark/blob/main/cookbook/README.md)**:
  - **[Local Development & Testing](https://github.com/karthick-jaganathan/streamwright-spark/blob/main/cookbook/01_local_development_and_testing.md)**: Cluster teardown, startup, data seeding, host execution (`sw spark run`), and Trino verification.
  - **[Delta Lake Merge & Catalog](https://github.com/karthick-jaganathan/streamwright-spark/blob/main/cookbook/02_delta_lake_merge_and_catalog.md)**: Dynamic partition pruning, explicit `when_matched` column mapping, Metastore registration, and 0-overhead driver caching.
  - **[Production Deployment & S3 Artifacts](https://github.com/karthick-jaganathan/streamwright-spark/blob/main/cookbook/03_production_deployment_s3_artifacts.md)**: The blind submission pattern on EMR/Kubernetes using dynamic wheel bootstrapping.
- **[Example Pipeline](https://github.com/karthick-jaganathan/streamwright-spark/blob/main/examples/s3_events_to_delta/README.md)**: Complete end-to-end event-driven ingestion pipeline with sample data.
