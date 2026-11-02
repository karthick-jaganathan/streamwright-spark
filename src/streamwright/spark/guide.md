# StreamWright Spark

SparkJob pipelines run on Spark with a shared StreamWright jobs control plane. Supported
platforms are **local**, **standalone**, and **emr_ec2** (install the `emr` extra for AWS).

## Pipeline

Declare `kind: SparkJob`, `name`, `spec.config`/`spec.secrets`, `connections`,
`steps`, `outputs`, and `runtime`. Steps create lazy DataFrames and views; outputs
trigger writes. The core Runner acquires a fenced lease, pins input messages and
reference values, records output receipts, commits the outbox atomically, acknowledges
messages, then publishes outbox events. Failed batches require `state retry` or
`state abandon`; Delta append replays check their transaction log before writing.

Connection credentials must use `{{ secrets.NAME }}` only in the adapter's declared
credential fields (or `runtime.env.STREAMWRIGHT_SECRET_*`); other values cannot
interpolate secrets. Provide non-secret inputs with `--config` or `--set NAME=VALUE`.
Configuration-derived JDBC host, database and port are checked again after resolution.

## Call steps (any source connector)

A SparkJob reaches **every** connector (connector contract v1.2 §6a). A connector with a
native Spark adapter runs distributed reads and writes; a source-only connector (the ads
and HTTP connectors) runs as a **call step**: the driver executes it through the source
engine (`SourceRunner.collect`) and streams its records into a DataFrame in bounded
batches (`page_size`). A call step has the same shape as a DuckDBJob call step — a
`connection`, then `service`/`method` with `arguments` (or an HTTP `request`) and an
optional `sql` over `input`:

```yaml
- name: raw_campaigns
  connection: google_ads_api        # provider: google_ads (source-only)
  service: GoogleAdsService
  method: search_stream
  arguments: {customer_id: "123", query: "SELECT campaign.id FROM campaign"}
  sql: SELECT * FROM input
```

`sw connectors` shows this reach, e.g. `google_ads  [source, duckdb (call), spark (call)]`.

`partitions: [{from, field, name}]` runs the call once per distinct, non-null value of a column of an earlier step
(crossed when there are several; the values are pinned in the batch, so a replay calls with the same ones), read as
`{{ partition.<name> }}` (`name` defaults to `field`) — e.g. read accounts, then each account's orders:

```yaml
- name: accounts
  connection: api
  request: {path: /v1/accounts, method: GET}
  sql: SELECT id AS account_id FROM input
- name: orders
  connection: api
  partitions: [{from: accounts, field: account_id}]
  request: {path: "/v1/accounts/{{ partition.account_id }}/orders", method: GET}
  retry: {codes: [429, 503], max_attempts: 5}
  sql: SELECT * FROM input
```

## File names

A file output (`service: object`, `method: write`) writes Spark's own `part-*` files into the folder `path`, unless a
`file_name` template names them: one file, or with `split_by` one file per split, under the folders of `layout`
(templates of `streamwright.core.jobs.naming`: `{today:%Y%m%d}`, `{now}`, `{batch_id}`, `{seq}`, a split column):

```yaml
- name: save_orders
  connection: lake
  service: object
  method: write
  arguments: {path: exports/orders/, format: csv, mode: overwrite,
              layout: "{today:%Y/%m}", file_name: "orders_{today:%Y%m%d}.csv"}
  sql: SELECT * FROM orders
```

## Execution policy

Steps and outputs take the one execution-policy vocabulary (the same keys and defaults as
a source stream and a DuckDBJob): `retry {max_attempts, backoff, max_delay, codes}`,
`rate_limit {requests, per}`, `paginator` / `page_size` (call steps), `timeout`, and
`on_error: fail | skip | warn`. A transient read or write retries through the one shared
retry; `on_error` `skip`/`warn` tolerate a failed step or output (`fail` is the default).
A key the Spark engine does not honour on a surface is an `unsupported-argument` error
naming the engine (for example `rate_limit` on an output).

## Commands

* `sw spark validate JOB [--overlay FILE] [--strict] [--analyze]`
* `sw spark plan JOB`
* `sw spark run JOB [--once | --drain | --available-now | --follow]`
* `sw spark lock JOB --spark VERSION`
* `sw spark state init|status|batches|show|retry|abandon|purge|lease JOB`

`streamwright-spark` and `sw-spark` expose the same commands. Installing this
distribution also registers `sw spark` and `kind: SparkJob` with the core CLI.
Local relative `runtime.state.sqlite` files live beside the job; remote jobs
must use an absolute SQLite path or a state connection.
New connectors declare their operations once in `streamwright.core.sdk.contract.ConnectorDefinition`
and register `CONNECTOR` in the `streamwright.connectors` entry-point group. A Spark
adapter implements `streamwright.spark.sdk.SparkAdapter` with `hadoop_settings`,
`read` and `write`; its class path goes in `CONNECTOR.adapters["spark"]`.
`SparkContext` provides the current batch runtime, step/output name and resolved
connection. Object-store adapters compose `streamwright.spark.io.files.read_files`
and `write_files` with a frozen `FileLocation`.
The engine loads the declared Spark adapter only when needed. Connectors must use
`streamwright.connectors` definitions; Spark-specific entry points and the old
`Provider`/method-table API are no longer supported.
