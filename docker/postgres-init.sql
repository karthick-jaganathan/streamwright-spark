-- Test environment only. One database holds the three roles the production job keeps apart:
--   sw_ops     bookkeeping (runtime.state)        reporting   business writes        ingest   mappings and vocabulary
CREATE SCHEMA IF NOT EXISTS sw_ops;
CREATE SCHEMA IF NOT EXISTS reporting;
CREATE SCHEMA IF NOT EXISTS ingest;

CREATE TABLE IF NOT EXISTS ingest.canonical_columns (
    dataset text NOT NULL, name text NOT NULL, type text NOT NULL, PRIMARY KEY (dataset, name));

CREATE TABLE IF NOT EXISTS ingest.column_mappings (
    dataset text NOT NULL, prefix text NOT NULL, header text NOT NULL, canonical text NOT NULL, type text NOT NULL,
    required boolean NOT NULL DEFAULT false, active boolean NOT NULL DEFAULT true,
    PRIMARY KEY (dataset, prefix, header));

CREATE TABLE IF NOT EXISTS reporting.ingest_audit (
    batch_id text NOT NULL, client_id text NOT NULL, row_count bigint, revenue double precision,
    PRIMARY KEY (batch_id, client_id));
CREATE TABLE IF NOT EXISTS reporting.ingest_audit_stg (LIKE reporting.ingest_audit);

CREATE TABLE IF NOT EXISTS reporting.file_event_audit (
    file_uri text PRIMARY KEY,
    client_id text,
    status text,
    batch_id text,
    received_at timestamp,
    rows_written bigint,
    delta_version bigint,
    completed_at timestamp);
CREATE TABLE IF NOT EXISTS reporting.file_event_audit_stg (LIKE reporting.file_event_audit);

INSERT INTO ingest.canonical_columns (dataset, name, type) VALUES
    ('orders', 'order_id', 'string'), ('orders', 'event_ts', 'timestamp'),
    ('orders', 'amo_id', 'string'), ('orders', 'revenue', 'double'), ('orders', 'cart_additions', 'int'),
    ('orders', 'custom_event_1', 'int'), ('orders', 'std_event_1', 'int')
ON CONFLICT DO NOTHING;

-- event_date is derived by the job from event_ts (tagged_orders), so it is not a mappable column
-- two clients with different headers for the same meaning, and one that maps cart additions to another name
INSERT INTO ingest.column_mappings (dataset, prefix, header, canonical, type, required) VALUES
    ('orders', 's3://acme-landing/clientA/', 'order id',        'order_id',       'string',    true),
    ('orders', 's3://acme-landing/clientA/', 'timestamp',       'event_ts',       'timestamp', true),
    ('orders', 's3://acme-landing/clientA/', 'AMO ID (evar8)',  'amo_id',         'string',    false),
    ('orders', 's3://acme-landing/clientA/', 'revenue',         'revenue',        'double',    false),
    ('orders', 's3://acme-landing/clientA/', 'cart additions',  'std_event_1',    'int',       false),
    ('orders', 's3://acme-landing/clientB/', 'OrderId',         'order_id',       'string',    true),
    ('orders', 's3://acme-landing/clientB/', 'timestamp',       'event_ts',       'timestamp', true),
    ('orders', 's3://acme-landing/clientB/', 'AMO_ID (evar18)', 'amo_id',         'string',    false),
    ('orders', 's3://acme-landing/clientB/', 'Revenue',         'revenue',        'double',    false),
    ('orders', 's3://acme-landing/clientB/', 'CartAdditions',   'custom_event_1', 'int',       false)
ON CONFLICT DO NOTHING;
