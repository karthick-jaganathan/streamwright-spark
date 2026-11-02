# Cookbook: Delta Lake Merge & Declarative Catalog Registration

This recipe details the architecture and syntax for performing high-performance, partition-pruned Delta Lake merges, declarative catalog registration in Hive Metastore, and fine-grained column mapping.

---

## 1. Dynamic Partition Pruning (`target_predicate`)

### Problem
In large-scale data lakes, a standard Delta `MERGE INTO` without partition filters forces Spark to scan all historical files in the table to evaluate the merge condition (`target.order_id = source.order_id`). On tables with millions of files, this causes massive S3 GET request costs, executor memory exhaustion, and slow jobs.

### Solution
StreamWright Spark allows declaring a `target_predicate` with pinned upstream values:

```yaml
outputs:
  - name: merge_orders
    connection: lake_delta
    service: table
    method: write
    arguments:
      mode: merge
      path: "s3a://{{ config.lake_bucket }}/delta/orders"
      partition_by: [client_id, event_date]
      condition: "target.order_id = source.order_id AND target.client_id = source.client_id"
      target_predicate: "target.event_date IN ({{ processed_orders.event_date }})"
```

- `{{ processed_orders.event_date }}` evaluates the distinct dates present in the current incoming micro-batch (e.g. `'2026-10-17'`).
- The generated merge query becomes:
  ```sql
  MERGE INTO delta.`s3a://.../orders` AS target
  USING source
  ON (target.event_date IN ('2026-10-17')) AND (target.order_id = source.order_id AND target.client_id = source.client_id)
  ...
  ```
- Delta Lake prunes partitions ahead of file scans, eliminating >95% of S3 I/O.

---

## 2. Explicit Column-Level `set` Mapping

When updating existing rows on match, you can specify exact column-level expressions under `set`, or omit `set` to default to `updateAll`:

### Option A: Explicit Column Mapping via `set` (Recommended)
Guarantees that immutable columns (such as created timestamps or original source files) are preserved, and only intended fields are updated:

```yaml
when_matched:
  - action: update
    condition: "target.event_ts < source.event_ts" # Optional predicate
    set:
      item_name: source.item_name
      amount: source.amount
      tax: source.tax
      total_amount: source.total_amount
      event_ts: source.event_ts
      source_file: source.source_file
      batch_id: source.batch_id
```

### Option B: Unconditional `updateAll`
If `set` is omitted, StreamWright invokes Delta's `whenMatchedUpdateAll()`:

```yaml
when_matched:
  - action: update
```

### Insert Clause (`when_not_matched`)
Similarly, `when_not_matched` supports inserting all columns or specifying explicit mappings via `values`:

```yaml
when_not_matched:
  - action: insert
```

---

## 3. Declarative Metastore Registration

StreamWright automatically manages catalog registration in Hive Metastore or DeltaCatalog directly from the output definition:

```yaml
outputs:
  - name: merge_orders
    connection: lake_delta
    service: table
    method: write
    arguments:
      mode: merge
      path: "s3a://{{ config.lake_bucket }}/delta/orders"
      table_name: orders
      schema: analytics
      schema_location: "s3a://{{ config.lake_bucket }}/analytics"
      schema_comment: "Central enterprise analytics data mart for orders and reporting"
      table_properties:
        "delta.autoOptimize.optimizeWrite": "true"
        "delta.autoOptimize.autoCompact": "true"
      comment: "Central orders table with partitioned Bronze storage"
      partition_by: [client_id, event_date]
```

### Features
1. **Custom Schema Location & Comments**:
   Executes `CREATE SCHEMA IF NOT EXISTS analytics COMMENT '...' LOCATION 's3a://.../analytics'`.
2. **External Table Registration**:
   Registers the table path as `analytics.orders` with Delta serde and Hive Metastore compatibility.
3. **Delta Table Properties (`TBLPROPERTIES`)**:
   Sets persistent properties like `delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact` on Delta transaction version 1.
4. **Zero Latency Overhead (Driver In-Memory Caching)**:
   The Spark driver caches registered schemas and tables in memory. On batch 1, DDL checks are performed; on batches 2..N, DDL checks are skipped completely with **0-ms overhead**.
