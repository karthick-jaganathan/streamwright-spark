# StreamWright Spark

SparkJob pipelines run on Spark with a shared StreamWright jobs control plane. Supported
platforms are **local**, **standalone**, and **emr_ec2** (install the `emr` extra for AWS).

## Pipeline

Declare `kind: SparkJob`, `name`, `spec.config`/`spec.secrets`, `connections`,
`steps`, `outputs`, and `runtime`. Steps create lazy DataFrames and views; outputs
trigger writes. The core Runner acquires a fenced lease, pins input messages and
reference values, records output receipts, commits the outbox atomically, acknowledges
messages, then publishes outbox events. A failed batch is dealt with by its scope's next
run (`runtime.state.failed_job_policy`): `abandon` (default) gives it up, `retry` resumes it
(up to `max_job_attempts`, default 3), `block` stops the scope until `state retry` or
`state abandon` (see State, scopes and locks); Delta append replays check their
transaction log before writing.

Connection credentials must use `{{ secrets.NAME }}` only in the adapter's declared
credential fields (or `runtime.env.STREAMWRIGHT_SECRET_*`); other values cannot
interpolate secrets. Provide non-secret inputs with `--config` or `--set NAME=VALUE`.
Configuration-derived JDBC host, database and port are checked again after resolution.

## Call steps (any source connector)

A SparkJob reaches **every** connector (connector contract v1.2 §6a). A connector with a
native Spark adapter runs distributed reads and writes; a source-only connector (the ads
and HTTP connectors) runs as a **call step**: the driver executes it through the source
engine (`SourceRunner.collect`) and then parallelizes its records into a DataFrame.
The result is currently collected and JSON-serialized on the driver, so driver memory
grows with the full result size; `page_size` sets Spark partitions and does not bound
driver memory. A call step has the same shape as a DuckDBJob call step — a
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
Setting `checkpoint: true` on a step requires `runtime.spark.conf.spark.checkpoint.dir`; use a directory accessible
to the Spark cluster.

## Lookups and accounts

`spec.lookups`, an input's `lookup: {from, field}` and `runtime.fan_out` work as for a DuckDBJob
(`get_engine_guide("DuckDBJob", "Lookups and accounts")`): credentials and account details are
looked up on the driver before anything connects, and the job runs once per account in its own
scope. A SparkJob runs its accounts one at a time (one SparkSession). On a cluster the lookups run
on the driver: the command must exist there, the function must be in the shipped environment.
`sw spark run JOB --set tenant_id=t1 --allow-lookup-exec` (forwarded to the driver).

The driver's log lines carry the same log context and `runtime.log.labels` as a DuckDBJob's
(`get_engine_guide("DuckDBJob", "Log context and labels")`); JVM lines do not.

## State, scopes and locks

`runtime.state` is `sqlite: PATH` (beside the job when relative; absolute on remote
platforms) or `connection: NAME` of a PostgreSQL or MySQL connection (`schema`, default
`streamwright`; the driver uses psycopg or PyMySQL for it, Spark uses JDBC for data), plus
`ttl_seconds`, `scope`, `lock`, `job_key`, `failed_job_policy` (what the next run does with
a failed batch: `abandon`, the default, gives it up, its error kept, and runs on; `retry`
resumes it from where it failed, with its pinned inputs, while it has been started fewer
than `max_job_attempts` times, default 3, then stops; `block` stops until `state retry` or
`state abandon`; an abandoned batch keeps its `job_key`, so use `retry` for keyed jobs) and
`apply_migrations`. The tables are the `sw_*`
tables of every engine. A run (and a state action) only checks them and fails (exit 1)
while a migration is pending: a deployment applies them with `sw spark state migrate JOB`
or `streamwright state migrate --url URL`; `run --apply-state-migrations` (forwarded to a
cluster driver) or `apply_migrations: true` (a local overlay) let a run apply them itself. `scope` and `lock` mean what they mean for a
DuckDBJob (`get_engine_guide("DuckDBJob", "State, scopes and locks")` has the full rules):

```yaml
spec:
  config:
    client:   {type: string}
    accounts: {type: list, items: string}      # --set accounts=111,222
runtime:
  state:
    connection: state_db
    scope: "client:{{ config.client }}"        # batches, recovery, --job-key, sequences, outbox: per client
    lock: {each: "{{ config.accounts }}", as: account, token: "account:{{ account }}", policy: prune}
```

- One run per scope is active; scopes run side by side; `lock.each` tokens keep two runs
  off the same item. `policy` `fail` (default), `wait` (with `timeout`), `prune` (narrow
  the list input to the free items; a run left with none is recorded `skipped`, exit 0);
  `--state-lock-policy` / `--state-lock-timeout` override it on `sw spark run`, and are
  forwarded to the driver on a cluster.
- A batch pins its inputs (scope, config values, item tokens): a resumed or retried batch
  rebuilds its steps from them on the driver.
- `runtime.state.job_key` (or `--job-key K`, which overrides it) runs a batch once per scope.
  `{{ batch.seq }}` counts per scope and `{{ batch.id }}` carries the scope hash: name shared
  paths with `{{ batch.id }}` or the scope's config value, so scopes never overwrite each
  other's files. Delta `txnAppId` is
  `<job>:<output>:<store id>[:<scope>]`.
- `sw spark state` has the DuckDBJob state actions: `list JOB [--scope TEXT] [--limit N]
  [--json]` (sequence, scope, rows, Spark app and host per batch), `status`, `show JOB JOB_ID`
  (pinned inputs, tokens, rows, receipts), `retry JOB JOB_ID` (resumes at the next run),
  `abandon JOB JOB_ID`, `leases`, `purge JOB --older-than 30d`, `migrate [--dry-run]`, `check`.
  retry and abandon take the batch's tokens (they fail while a run of its scope holds them).
  Every action takes `JOB [--overlay F] [--config F] [--set K=V] [--secrets F]` (only the
  state connection's secrets) and its own options.

## Commands

* `sw spark validate JOB [--overlay FILE] [--strict] [--analyze]`
* `sw spark plan JOB`
* `sw spark run JOB [--once | --drain | --available-now | --follow [--interval 10s]] [--job-key K]
  [--state-lock-policy fail|wait|prune] [--state-lock-timeout 5m] [--summary [FILE]]
  [--failed-job-policy abandon|retry|block] [--max-job-attempts N] [--apply-state-migrations]
  [--log-level LEVEL] [--log NAME=LEVEL] [--spark-log-level WARN]
  [--dry-run] [--detach]`: a run only checks the state tables (`sw spark state migrate JOB`
  creates them; `--apply-state-migrations` lets the run do it);
  `--log-level`/`--log`/`--log-format` set streamwright's loggers (as on every command),
  `--spark-log-level` the JVM's; `--interval` needs `--follow`, `--detach` a remote platform;
  `--summary` writes the run's JSON summary (status `ok`, `skipped` or `failed`) on the driver.
* `sw spark lock JOB --spark VERSION [--check]` (the dependency lock file, checked by
  `run --lock F` / `--lock-required`; not `runtime.state.lock`): `--check` verifies the
  installed versions and that the lock still pins what the job's connectors need
* `sw spark state ACTION JOB` (see State, scopes and locks)

Every option, live: `sw spark <command> --help` (MCP: `get_cli_reference("spark run")`).

`streamwright-spark` and `sw-spark` expose the same commands. Installing this
distribution also registers `sw spark` and `kind: SparkJob` with the core CLI.
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
