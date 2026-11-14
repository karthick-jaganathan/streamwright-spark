#!/bin/sh
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

# Seeds sample partner feeds and audit tables for the s3_partner_orders pipeline.
# Run from repository root after `make up`:
set -eu

dc="docker compose"

echo "==> Ensuring S3 buckets exist in LocalStack..."
for bucket in acme-landing acme-lake code-bucket; do
  $dc exec -T localstack awslocal s3api head-bucket --bucket "$bucket" >/dev/null 2>&1 \
    || $dc exec -T localstack awslocal s3 mb "s3://$bucket"
done

echo "==> Creating PostgreSQL audit schema and tables in Postgres..."
$dc exec -T postgres psql -U sw -d streamwright <<'SQL'
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.schema_drift_events (
    step TEXT,
    file_uri TEXT,
    matched_partner_prefix TEXT,
    status TEXT,
    header_hash TEXT,
    unmapped_columns TEXT,
    missing_optional_columns TEXT,
    batch_id TEXT,
    detected_at TIMESTAMP,
    PRIMARY KEY (batch_id, step, file_uri)
);

CREATE TABLE IF NOT EXISTS audit.file_rejection_events (
    step TEXT,
    file_uri TEXT,
    reason TEXT,
    batch_id TEXT,
    rejected_at TIMESTAMP,
    PRIMARY KEY (batch_id, step, file_uri)
);

-- tables made by an earlier seed had no key: the job's merge outputs (ON CONFLICT) need it
DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['schema_drift_events', 'file_rejection_events'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = ('audit.' || t)::regclass AND contype = 'p') THEN
            EXECUTE format('DELETE FROM audit.%I WHERE batch_id IS NULL OR step IS NULL OR file_uri IS NULL', t);
            EXECUTE format('DELETE FROM audit.%I a USING audit.%I b WHERE a.ctid < b.ctid AND a.batch_id = b.batch_id '
                           'AND a.step = b.step AND a.file_uri = b.file_uri', t, t);
            EXECUTE format('ALTER TABLE audit.%I ADD PRIMARY KEY (batch_id, step, file_uri)', t);
        END IF;
    END LOOP;
END $$;
SQL

echo "==> Uploading partner test files to LocalStack S3..."
$dc exec -T localstack sh -c '
# Partner Alpha: CSV with header casing, dynamic metric/attribute, unmapped drift (promo_code), and corrupted row
cat <<EOF > /tmp/alpha_orders_01.csv
Order_Num,Cust_ID,AmountUSD,EventDate,fee_processing,attr_region,promo_code
1001,CUST-A,149.99,2026-10-18 10:00:00,4.50,US-EAST,SUMMER10
1002,CUST-B,bad_amount,2026-10-18 10:05:00,2.50,US-WEST,FALL20
EOF
awslocal s3 cp /tmp/alpha_orders_01.csv s3://acme-landing/partners/alpha/alpha_orders_01.csv

# Partner Beta: TSV with transaction_id, client_key, dynamic shipping metric and channel attribute
cat <<EOF > /tmp/beta_orders_01.tsv
txn_id	client_key	total	timestamp	metric_shipping	attr_channel
2001	BETA-1	89.50	2026-10-18 11:00:00	12.00	WEB
EOF
awslocal s3 cp /tmp/beta_orders_01.tsv s3://acme-landing/partners/beta/beta_orders_01.tsv

# Unknown Partner: File dropped in unrecognized path (triggers rejection into _file_errors)
cat <<EOF > /tmp/rejected_01.csv
foo,bar,baz
1,2,3
EOF
awslocal s3 cp /tmp/rejected_01.csv s3://acme-landing/partners/unknown/rejected_01.csv
'

echo "==> Seed completed successfully for s3_partner_orders!"
