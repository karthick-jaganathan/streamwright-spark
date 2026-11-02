#!/bin/sh
set -eu

# the wheels publish_s3.sh uploaded (whatever their versions), as spark-submit --py-files
PY_FILES=$(ls /tmp/sw_wheels/*.whl | xargs -n1 basename | sed "s|^|s3a://code-bucket/packages/|" | paste -sd, -)
[ -n "$PY_FILES" ] || { echo "no wheels in /tmp/sw_wheels: run publish_s3.sh first" >&2; exit 1; }

echo "==> Running blind spark-submit with S3 artifacts..."
docker run --rm \
  --network streamwright-spark_default \
  -e AWS_ACCESS_KEY_ID=test \
  -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e AWS_ENDPOINT_URL=http://localstack:4566 \
  -e STREAMWRIGHT_SECRET_OPS_DB_PASSWORD=change-me-locally \
  -e STREAMWRIGHT_SECRET_REPORTING_DB_PASSWORD=change-me-locally \
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
  --py-files "$PY_FILES" \
  s3a://code-bucket/entrypoint.py \
  --job s3://code-bucket/jobs/kafka/order_files.yaml \
  --overlay s3://code-bucket/jobs/kafka/order_files.docker.yaml \
  --set kafka_bootstrap=kafka:9092 \
  --set lake_bucket=acme-lake \
  --once "$@"
