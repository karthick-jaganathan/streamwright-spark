# Cookbook: Production Deployment & S3 Artifact Serving

This recipe explains how to deploy and operate StreamWright Spark pipelines in production cloud environments (AWS EMR on EC2, EMR Serverless, Amazon EKS, Databricks) using the **blind submission pattern**.

---

## 1. The Clean Container & Blind Submit Principle

### Traditional Approach (Anti-Pattern)
- Pre-installing pipeline application code, connectors, and configurations into custom Docker images or cluster AMIs.
- Requires rebuilding and redeploying entire container images or spinning up new clusters for every pipeline change or bugfix.

### StreamWright Approach (Blind Submission)
- The cluster runtime or container (`spark-clean:latest`) contains only:
  - Base Apache Spark and PySpark.
  - Standard platform JARs (Delta Lake, PostgreSQL JDBC, Hadoop-AWS, AWS SDK).
  - Common runtime dependencies (`pyyaml`, `psycopg`, `boto3`, `confluent-kafka`).
  - **Zero** StreamWright code pre-installed.
- All application packages (`streamwright`, `streamwright_spark`, connector packages), the driver entrypoint, and YAML pipeline configurations are served dynamically from an S3 code bucket (`s3://code-bucket/`).

---

## 2. Dynamic Runtime Bootstrapping (`entrypoint.py`)

When Spark distributes Python wheels via `--py-files`, PEP 420 namespace packages (`streamwright.*`) and CLI entry points can face import isolation issues on worker nodes.

StreamWright solves this via [`src/streamwright/spark/entrypoint.py`](../src/streamwright/spark/entrypoint.py):

```python
def _bootstrap_wheels():
    """Unpack wheels passed via --py-files into a local directory so
    PEP 420 namespace packages and entry points resolve cleanly."""
    unpacked = os.path.abspath("./.wheels")
    has_wheels = False
    for path in list(sys.path):
        if path.endswith(".whl") and os.path.isfile(path):
            has_wheels = True
            with zipfile.ZipFile(path) as archive:
                archive.extractall(unpacked)
    if has_wheels and unpacked not in sys.path:
        sys.path.insert(0, unpacked)

if __name__ == "__main__":
    _bootstrap_wheels()
    from streamwright.spark.cli import main
    raise SystemExit(main(["run", "--driver"] + sys.argv[1:]))
```

---

## 3. Production Submission Command

To submit a pipeline to an EMR cluster or clean container:

```bash
spark-submit \
  --master "yarn" \
  --deploy-mode "cluster" \
  --conf spark.sql.extensions="io.delta.sql.DeltaSparkSessionExtension" \
  --conf spark.sql.catalog.spark_catalog="org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf spark.sql.catalogImplementation="hive" \
  --py-files \
s3a://my-code-bucket/packages/streamwright-0.1.8-py3-none-any.whl,\
s3a://my-code-bucket/packages/streamwright_spark-0.0.0-py3-none-any.whl,\
s3a://my-code-bucket/packages/streamwright_s3-0.0.1-py3-none-any.whl,\
s3a://my-code-bucket/packages/streamwright_postgres-0.0.1-py3-none-any.whl,\
s3a://my-code-bucket/packages/streamwright_deltalake-0.0.1-py3-none-any.whl,\
s3a://my-code-bucket/packages/streamwright_kafka-0.0.1-py3-none-any.whl,\
s3a://my-code-bucket/packages/delta_spark-3.2.1-py3-none-any.whl \
  s3a://my-code-bucket/entrypoint.py \
  --job s3://my-code-bucket/jobs/s3_events_to_delta/job.yaml \
  --overlay s3://my-code-bucket/jobs/s3_events_to_delta/emr.yaml \
  --set kafka_bootstrap=b-1.my-cluster.kafka.us-east-1.amazonaws.com:9092 \
  --set lake_bucket=my-prod-lake \
  --once
```

### Key Advantages
1. **Instant Pipeline Deployments**: Updating a pipeline requires only syncing `job.yaml` and `.whl` files to S3. No cluster restarts or image builds needed.
2. **Immutable Versioning**: Store jobs under versioned prefixes (e.g. `s3://my-code-bucket/jobs/orders/v1.2.0/`).
3. **Multi-Tenant Sharing**: The same Spark cluster or EMR environment can run completely different jobs with different connector requirements without dependency conflicts.
