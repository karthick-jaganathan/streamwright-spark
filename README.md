# StreamWright Spark

[![PyPI](https://img.shields.io/pypi/v/streamwright-spark.svg)](https://pypi.org/project/streamwright-spark/)
[![Python](https://img.shields.io/pypi/pyversions/streamwright-spark.svg)](https://pypi.org/project/streamwright-spark/)
[![License](https://img.shields.io/pypi/l/streamwright-spark.svg)](https://pypi.org/project/streamwright-spark/)
[![Docs](https://img.shields.io/badge/docs-streamwright.web.app-brightgreen.svg)](https://streamwright.web.app/docs/spark/overview/)

`kind: SparkJob` for [StreamWright](https://pypi.org/project/streamwright/): declarative batch pipelines on Apache Spark.

- **Read and write** object storage, Delta Lake, databases, Kafka and SQS with StreamWright connectors.
- **Exactly-once batches**: batch state, leases and output receipts; replay-safe writes.
- **Any source connector** (REST APIs, ads platforms) through driver-side call steps.
- **Runs** locally, on Spark standalone and on AWS EMR on EC2.

## Install

```bash
pip install "streamwright-spark[spark]"    # needs Java 11+
sw connectors -i files
```

## Example

```yaml
# orders.yaml
kind: SparkJob
version: 1
name: daily_orders
connections:
  lake: {provider: files, roots: ["/data/lake/"]}
steps:
  - name: orders
    connection: lake
    service: object
    method: read
    arguments: {path: "raw/orders/*.csv", format: csv, options: {header: true}}
  - name: daily_totals
    sql: SELECT CAST(ordered_at AS DATE) AS day, COUNT(*) AS orders FROM orders GROUP BY 1
outputs:
  - name: save_totals
    connection: lake
    service: object
    method: write
    arguments: {path: "/data/lake/curated/daily_totals/", format: parquet, mode: overwrite}
    sql: SELECT * FROM daily_totals
runtime:
  state: {sqlite: /data/state/daily_orders.db}
  platform: {type: local}
```

```bash
sw spark validate orders.yaml
sw spark run orders.yaml --once
```

## Links

[Documentation](https://streamwright.web.app/docs/spark/overview/) ·
[StreamWright](https://github.com/karthick-jaganathan/streamwright)
