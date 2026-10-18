#!/bin/sh
# Test data for the Docker environment (run from the repository root, after `make up`):
# Creates the Kafka topics, uploads sample files to the LocalStack landing bucket,
# and announces them on lake.s3_file_events.v1.
set -eu
dc="docker compose"
kafka_bin=/opt/kafka/bin

echo "==> Creating Kafka topics..."
for topic in lake.s3_file_events.v1 lake.orders.committed.v1 lake.file_arrived.v1 lake.bronze.committed.v1; do
  $dc exec -T kafka $kafka_bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
      --topic "$topic" --partitions 1 --replication-factor 1
done

echo "==> Ensuring S3 buckets exist in LocalStack..."
for bucket in acme-landing acme-lake code-bucket; do
  $dc exec -T localstack awslocal s3api head-bucket --bucket "$bucket" >/dev/null 2>&1 \
    || $dc exec -T localstack awslocal s3 mb "s3://$bucket"
done

echo "==> Uploading sample order files to LocalStack S3..."
put() {
  $dc exec -T localstack sh -c 'cat > /tmp/seed.csv && awslocal s3 cp /tmp/seed.csv "s3://$0"' "$1"
}

put acme-landing/orders_clientA_20261017.csv <<'CSV'
order_id,client_id,item_name,amount,event_ts
A-101,clientA,Wireless Mouse,29.99,2026-10-17 09:15:00
A-102,clientA,Mechanical Keyboard,89.50,2026-10-17 10:40:00
A-103,clientA,USB-C Hub,42.00,2026-10-16 23:59:00
CSV

put acme-landing/orders_clientB_20261017.csv <<'CSV'
order_id,client_id,item_name,amount,event_ts
B-201,clientB,4K Monitor,320.00,2026-10-17 08:00:00
B-202,clientB,Monitor Arm,55.25,2026-10-17 08:30:00
CSV

echo "==> Publishing S3 file arrival notification events to Kafka..."
printf '%s\n' \
  '{"client_id":"clientA","file_uri":"s3://acme-landing/orders_clientA_20261017.csv"}' \
  '{"client_id":"clientB","file_uri":"s3://acme-landing/orders_clientB_20261017.csv"}' \
| $dc exec -T kafka $kafka_bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic lake.s3_file_events.v1

echo "==> Seed completed successfully! You can now run the pipeline in Docker."
