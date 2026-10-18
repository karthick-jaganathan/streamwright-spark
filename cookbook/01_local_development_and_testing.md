# Cookbook: Local Development, Cluster Lifecycle & Testing

This recipe guides you through managing the local StreamWright Spark testing environment, from a completely clean wipe to cluster startup, data seeding, artifact publication, and pipeline execution.

---

## Architecture of the Local Stack

The local stack runs via Docker Compose with the `lakehouse` profile enabled:

| Service | Internal Address | Host Port | Purpose |
|---|---|---|---|
| **LocalStack** | `http://localstack:4566` | `127.0.0.1:4566` | S3 object storage (landing, lake, and code buckets) & SQS |
| **Kafka** | `kafka:9092` | `127.0.0.1:9094` | Event broker for file arrival and batch completion notifications |
| **PostgreSQL** | `postgres:5432` | `127.0.0.1:5432` | Bookkeeping and operational state database (`streamwright`) |
| **Hive Metastore** | `thrift://metastore:9083` | `127.0.0.1:9083` | Central catalog for Delta Lake table registration |
| **Trino** | `http://trino:8080` | `127.0.0.1:8088` | Distributed SQL query engine for Delta Lake tables |

---

## 1. Dropping the Cluster (Clean Slate)

To stop and remove all containers, networks, and persistent Docker volumes (purging all S3 data, Kafka topics, offsets, PostgreSQL audit records, and Metastore metadata):

```bash
make down
```

*Equivalent command:*
```bash
docker compose --profile lakehouse down -v
```

---

## 2. Bringing Up All Services

To start all 5 services and wait for their health checks to pass:

```bash
make lakehouse
```

*Equivalent command:*
```bash
docker compose --profile lakehouse up -d --wait
```

Verify that all containers report `healthy`:
```bash
docker compose ps
```

---

## 3. Seeding Initial Topics, Buckets, and Events

To automatically create Kafka topics, create S3 buckets (`acme-landing`, `acme-lake`, `code-bucket`), upload sample CSV order files, and emit arrival events to Kafka:

```bash
make seed
```

*Equivalent command:*
```bash
sh docker/seed.sh
```

---

## 4. Building & Publishing Artifacts to S3

To package `streamwright_spark` into a wheel and synchronize all code packages, the driver entrypoint, and job YAMLs to `s3://code-bucket/`:

```bash
make publish-s3
```

*Equivalent command:*
```bash
sh docker/publish_s3.sh
```

This ensures `s3://code-bucket/` contains:
- `s3://code-bucket/packages/*.whl`: Pure Python wheels for core, spark, connectors, and `delta-spark`.
- `s3://code-bucket/entrypoint.py`: Driver bootstrap entrypoint.
- `s3://code-bucket/jobs/s3_events_to_delta/job.yaml`: Job specification.
- `s3://code-bucket/jobs/s3_events_to_delta/overlay.yaml`: Docker runtime overlay.

---

## 5. Running the Pipeline

You can run the pipeline using either of two methods:

### Method A: Blind `spark-submit` in Clean Container (Simulates EMR/Production)
Runs inside `spark-clean:latest` (a bare Spark container with **zero** StreamWright packages pre-installed). Spark pulls all code and configuration dynamically from S3:

```bash
make submit-blind
```

*Equivalent command:*
```bash
sh docker/submit_blind.sh
```

### Method B: Direct Host CLI Run (From macOS Terminal)
Runs in-process using your local Python virtual environment, connecting to LocalStack at `127.0.0.1:4566`, Kafka at `127.0.0.1:9094`, PostgreSQL at `127.0.0.1:5432`, and Hive Metastore at `thrift://127.0.0.1:9083`:

```bash
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
AWS_DEFAULT_REGION=us-east-1 \
STREAMWRIGHT_SECRET_AWS_ACCESS_KEY_ID=test \
STREAMWRIGHT_SECRET_AWS_SECRET_ACCESS_KEY=test \
STREAMWRIGHT_SECRET_S3_KEY_ID=test \
STREAMWRIGHT_SECRET_S3_SECRET=test \
STREAMWRIGHT_SECRET_OPS_DB_PASSWORD=change-me-locally \
STREAMWRIGHT_SECRET_REPORTING_DB_PASSWORD=change-me-locally \
sw spark run examples/s3_events_to_delta \
  --overlay overlays/local.yaml \
  --set kafka_bootstrap=127.0.0.1:9094 \
  --set lake_bucket=acme-lake \
  --once
```

---

## 6. Querying & Verifying in Trino

### Interactive CLI
Launch an interactive Trino shell directly against the `delta` catalog and `analytics` schema:

```bash
make trino
```

### Direct Terminal SQL Queries
Query the merged Delta table:
```bash
docker compose --profile lakehouse exec -T trino trino \
  --catalog delta \
  --schema analytics \
  --execute "SELECT order_id, client_id, item_name, total_amount, batch_id FROM orders ORDER BY order_id;"
```

Inspect the Delta Lake ACID transaction history (showing `WRITE`, `SET TBLPROPERTIES`, and `MERGE` commits):
```bash
docker compose --profile lakehouse exec -T trino trino \
  --catalog delta \
  --schema analytics \
  --execute "SELECT version, timestamp, operation FROM \"orders\$history\" ORDER BY version DESC LIMIT 5;"
```
