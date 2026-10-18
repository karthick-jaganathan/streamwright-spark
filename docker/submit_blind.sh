#!/bin/sh
set -eu

echo "==> Running blind spark-submit with S3 artifacts..."
docker run --rm \
  --network streamwright-spark_default \
  -e AWS_ACCESS_KEY_ID=test \
  -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e AWS_ENDPOINT_URL=http://localstack:4566 \
  -e STREAMWRIGHT_SECRET_AWS_ACCESS_KEY_ID=test \
  -e STREAMWRIGHT_SECRET_AWS_SECRET_ACCESS_KEY=test \
  -e STREAMWRIGHT_SECRET_S3_KEY_ID=test \
  -e STREAMWRIGHT_SECRET_S3_SECRET=test \
  -e STREAMWRIGHT_SECRET_OPS_DB_PASSWORD=change-me-locally \
  -e STREAMWRIGHT_SECRET_REPORTING_DB_PASSWORD=change-me-locally \
  spark-clean:latest \
  --master "local[2]" \
  --conf spark.hadoop.fs.s3a.endpoint="http://localstack:4566" \
  --conf spark.hadoop.fs.s3a.path.style.access="true" \
  --conf spark.hadoop.fs.s3a.access.key="test" \
  --conf spark.hadoop.fs.s3a.secret.key="test" \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled="false" \
  --conf spark.hadoop.fs.s3a.impl="org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf spark.sql.extensions="io.delta.sql.DeltaSparkSessionExtension" \
  --conf spark.sql.catalog.spark_catalog="org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf spark.sql.catalogImplementation="hive" \
  --conf spark.hadoop.hive.metastore.uris="thrift://metastore:9083" \
  --py-files s3a://code-bucket/packages/streamwright-0.1.8-py3-none-any.whl,s3a://code-bucket/packages/streamwright_spark-0.0.0-py3-none-any.whl,s3a://code-bucket/packages/streamwright_s3-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_postgres-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_deltalake-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_kafka-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_sqs-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_files-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_gcs-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/streamwright_azure_blob-0.0.1-py3-none-any.whl,s3a://code-bucket/packages/delta_spark-3.2.1-py3-none-any.whl \
  s3a://code-bucket/entrypoint.py \
  --job s3://code-bucket/jobs/s3_events_to_delta/job.yaml \
  --overlay s3://code-bucket/jobs/s3_events_to_delta/overlay.yaml \
  --set kafka_bootstrap=kafka:9092 \
  --set lake_bucket=acme-lake \
  --once "$@"
