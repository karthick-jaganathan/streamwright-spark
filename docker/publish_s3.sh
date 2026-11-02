#!/bin/sh
set -eu

echo "==> Ensuring S3 code-bucket exists in LocalStack..."
docker compose exec -T localstack awslocal s3api head-bucket --bucket code-bucket >/dev/null 2>&1 \
  || docker compose exec -T localstack awslocal s3 mb s3://code-bucket

echo "==> Checking wheel packages in /tmp/sw_wheels..."
mkdir -p /tmp/sw_wheels
if [ ! -f /tmp/sw_wheels/delta_spark-3.2.1-py3-none-any.whl ]; then
  pip download delta-spark==3.2.1 --no-deps -d /tmp/sw_wheels/
fi

echo "==> Building streamwright-spark wheel..."
python3 -m build --wheel --outdir /tmp/sw_wheels/ .

echo "==> Uploading wheels to s3://code-bucket/packages/..."
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://127.0.0.1:4566 s3 sync /tmp/sw_wheels/ s3://code-bucket/packages/

echo "==> Uploading entrypoint.py and job definitions..."
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://127.0.0.1:4566 s3 cp src/streamwright/spark/entrypoint.py s3://code-bucket/entrypoint.py

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://127.0.0.1:4566 s3 cp examples/kafka/order_files.yaml s3://code-bucket/jobs/kafka/order_files.yaml

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://127.0.0.1:4566 s3 cp examples/kafka/order_files.docker.yaml s3://code-bucket/jobs/kafka/order_files.docker.yaml

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://127.0.0.1:4566 s3 cp examples/s3/partner_orders.yaml s3://code-bucket/jobs/s3/partner_orders.yaml

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://127.0.0.1:4566 s3 cp examples/s3/partner_orders.docker.yaml s3://code-bucket/jobs/s3/partner_orders.docker.yaml

echo "==> All artifacts published successfully to s3://code-bucket!"

