# StreamWright Spark

SparkJob pipelines run on Spark with a shared StreamWright jobs control plane. Supported
platforms are **local**, **standalone**, and **emr_ec2** (install the `emr` extra for AWS).

## Pipeline

Declare `kind: SparkJob`, `name`, `spec.config`/`spec.secrets`, `connections`,
`steps`, `outputs`, and `runtime`. `name` may have dotted parts (`s3.orders_export`); step, output
and connection names have none. Steps create lazy DataFrames and views; outputs
trigger writes. The core Runner acquires a fenced lease, pins input messages and
reference values, records output receipts, commits the outbox atomically, acknowledges
messages, then publishes outbox events. A failed batch is dealt with by its scope's next
run (`runtime.state.failed_job_policy`): `abandon` (default) gives it up, `retry` resumes it
(up to `max_job_attempts`, default 3), `block` stops the scope until `streamwright state retry` or
`abandon` (see State, scopes and locks); Delta append replays check their
transaction log before writing.

Connection credentials must use `{{ secrets.NAME }}` only in the adapter's declared
credential fields (or `runtime.env.STREAMWRIGHT_SECRET_*`); other values cannot
interpolate secrets. Provide non-secret inputs with `--config` or `--set NAME=VALUE`.
Configuration-derived JDBC host, database and port are checked again after resolution.

Secrets come from `--secrets FILE` or `STREAMWRIGHT_SECRET_<NAME>` locally. A cluster driver gets
neither: bind each secret it needs to a lookup (see Lookups and accounts), such as
`db_password: {type: string, lookup: {from: db, field: password}}` with
`spec.lookups.db: {type: aws_secretsmanager, secret_id: prod/database, region: us-east-1}`
(without `field`: the whole value). On a remote platform, `validate` requires it for every
secret `runtime.env` references (`secret-provider`). An explicit value wins over a lookup.

## Call steps (any source connector)

A SparkJob reaches **every** connector (connector contract v1.2 §6a). A connector with a
native Spark adapter runs distributed reads and writes; a source-only connector (the ads
and HTTP connectors) runs as a **call step**: the driver executes it through the source
engine (`SourceRunner.collect`) and then parallelizes its records into a DataFrame, `page_size`
records at a time (default 5000; Python holds one chunk, but the parallelized chunks stay in the
driver JVM until Spark reads them, so the result must still fit the driver's memory: at most
`runtime.limits.call_records` records per step and batch). A call
step has the same shape as a DuckDBJob call step — a `connection`, then
`operation: {service, method, arguments}` (or an HTTP request `{path, ...}`) and an optional
`sql` over `records`, one row per record with one `record` column, the record's JSON text
(Spark SQL reads it with `get_json_object` or `from_json`); its columns do not depend on how
many records came, so a batch that reads none still runs the `sql`. Without `sql` the step has
the records' own (inferred) columns:

```yaml
- name: raw_campaigns
  connection: google_ads_api        # provider: google_ads (source-only)
  operation:
    service: GoogleAdsService
    method: search_stream
    arguments: {customer_id: "123", query: "SELECT campaign.id FROM campaign"}
  sql: SELECT get_json_object(record, '$.campaign.id') AS campaign_id FROM records
```

`sw connectors` shows this reach, e.g. `google_ads  [source, duckdb (call), spark (call)]`.

`partitions: [{from, field, name}]` runs the call once per distinct, non-null value of a column of an earlier step
(crossed when there are several; the values are pinned in the batch, so a replay calls with the same ones), read as
`{{ partition.<name> }}` (`name` defaults to `field`) — e.g. read accounts, then each account's orders:

```yaml
- name: accounts
  connection: api
  operation: {path: /v1/accounts, method: GET}
  sql: SELECT get_json_object(record, '$.id') AS account_id FROM records
- name: orders
  connection: api
  partitions: [{from: accounts, field: account_id}]
  operation: {path: "/v1/accounts/{{ partition.account_id }}/orders", method: GET}
  policy:
    retry: {codes: [429, 503], max_attempts: 5}
```

A call step may take `async_job:` instead of `operation:` (one or the other) to run a report job on the driver:
`submit`, `poll` (`every`, `timeout`, `done_when`, `fail_when`) and exactly one of `results` or `download`, the
grammar of a source request's `async_job` and of DuckDBJob's call steps (`get_engine_guide("DuckDBJob", "Call
steps")`). Each part's `operation:` is a call (SDK connectors) or an HTTP request (REST connectors); its records reach
`records` like a call's and count toward `call_records`. Call steps have no windows: set the range with
`{{ config.* }}` or partitions.

```yaml
- name: report
  connection: ads                   # provider: microsoft_ads
  async_job:
    submit:
      operation:
        service: ReportingService
        method: SubmitGenerateReport
        arguments:
          request:
            ReportRequest:
              "@type": CampaignPerformanceReportRequest
              Aggregation: Summary
              Format: Csv
              Columns: [AccountId, CampaignId, Impressions, Clicks, Spend]
              Scope: {AccountIds: ["{{ config.account_id }}"]}
              Time: {PredefinedTime: Last30Days}
    poll:
      operation: {method: PollGenerateReport, arguments: {request: {ReportRequestId: "{{ submit.result }}"}}}
      every: 15s
      timeout: 30m
      done_when: {path: Status, equals: Success}
      fail_when: {path: Status, equals: Error}
    download: {url: "{{ poll.ReportDownloadUrl }}", format: csv, compression: zip}
  sql: SELECT get_json_object(record, '$.CampaignId') AS campaign_id FROM records
```

## Source steps

A `source:` + `stream:` step runs one stream of a StreamWright source folder on the driver, as a DuckDBJob's source
step does: `{name, source, stream, config, partitions, sql}`. Use it for a report or sync that is already a source
stream (incremental windows, lookback, bookmarks), or shared with other jobs.

```yaml
- name: performance
  source: ../sources/microsoft_ads  # the source folder
  stream: campaign_performance
  config: {account_ids: ["{{ config.account_id }}"], customer_id: "{{ config.customer_id }}"}
  sql: SELECT get_json_object(record, '$.campaign_id') AS campaign_id FROM records
```

- The rows are the stream's export rows: with `sql`, as the `record` JSON column of `records`; without, as their own
  columns. A run without rows keeps the export's columns, so later steps still resolve them.
- The records are collected on the driver, with call steps' cap (`runtime.limits.call_records`). Bookmarks are kept
  per partition scope, so each scope resumes from its own.
- `policy` takes `retry`, `page_size`, `on_error` and `timeout`; the source folder owns its paging and rate limit.
- Where the folder is: a local path (a relative one starts at the job file's folder), or an `s3://` folder. When the
  job manifest is itself an `s3://` URI, a relative path is resolved under the manifest's prefix (never outside its
  bucket). The driver downloads an `s3://` folder's YAML files (`source.yaml`, `streams/*.yaml`), up to 5 MiB in all.

## Apply outputs (writes to a vendor API)

An output whose connection is a connector (no Spark adapter) and whose `operation:` is one of its apply
operations - Google Ads `*Service.mutate_*`, `ConversionUploadService.upload_click_conversions`, ... - sends its rows
to the vendor, one operation per row (`sql` gives the rows; they are collected on the driver, at most
`runtime.limits.apply_rows`, default 100000). The shape, policies and delivery are DuckDBJob's, shared:
`apply: {chunk_size, on_row_error: fail|continue, nulls: error|skip_field|clear, on_unknown: hold|reconcile,
allow_remove}`, `policy: {retry, rate_limit}` and `log: {sql, message, level, max_rows}` (once the rows are sent, a
line per row of a SELECT over the output's results: its columns and `outcome.status|error_code|error_message|result|row`);
rows are committed to the outbox with the batch, each chunk is marked in flight
before its call and every row's outcome kept (`streamwright state show --url URL JOB_ID`). A call cut short is sent
again for an idempotent operation, else held until `streamwright state resend|skip --url URL JOB_ID` (MCP:
`get_engine_guide("duckdb", "Apply outputs")` has the details).

## File names

A file output (`operation: {service: object, method: write}`) writes Spark's own `part-*` files into the folder `path`, unless a
`file_name` template names them: one file, or with `split_by` one file per split, under the folders of `layout`
(templates of `streamwright.core.jobs.naming`: `{today:%Y%m%d}`, `{now}`, `{batch_id}`, `{seq}`, a split column):

```yaml
- name: save_orders
  connection: lake
  operation:
    service: object
    method: write
    arguments: {path: exports/orders/, format: csv, mode: overwrite,
                layout: "{today:%Y/%m}", file_name: "orders_{today:%Y%m%d}.csv"}
  sql: SELECT * FROM orders
```

`mode: append` is replay-safe only for named files of the batch (`{batch_id}` in a name, or `{{ batch.id }}` in
`path`): Spark's own `part-*` files get new names on every attempt, so a replayed batch would add its rows again;
write them to a folder of the batch with `mode: overwrite` (`path: exports/orders/{{ batch.id }}/`).

## Templates and limits

Paths, file names and predicates (`replace_where`, `target_predicate`) render `{{ batch.id }}`, `{{ batch.ts }}`,
`{{ batch.id_safe }}`, `{{ batch.seq }}` and `{{ driver.host }}`; `sql` is never templated: it takes the `:batch_id`,
`:batch_ts` and `:driver_host` parameters (and `spec.config` names), and a `{{ }}` constant in it is an error. An
adapter's own tokens (a JDBC `staging_table`'s `{{ uuid }}`, `{{ output.name }}`) are accepted only in the arguments
of that adapter's outputs. `runtime.limits` takes DuckDBJob's keys with their meaning: `reference_values` (10000),
`read_groups` (200), `events` (the rows of one publish output, 10000) and `apply_rows` (100000), plus
two Spark-only keys: `max_files_per_batch` (2000, one file read) and `call_records` (1000000, the records one call
step may collect on the driver per batch, over all its partition scopes; DuckDBJob streams a call step's records to
its table in chunks and has no such cap). A call step that passes `call_records` fails as soon as it does, before
reading on, naming the step and the count reached; any other key is an error naming the Spark engine.

## Execution policy

Steps and outputs take the one execution-policy vocabulary in their `policy:` group (the same
keys and defaults as a source stream's `policy:` and a DuckDBJob's): `retry {max_attempts,
backoff, max_delay, codes}`, `rate_limit {requests, per}`, `page_size` (call steps; an HTTP
step's `paginator` is in its `operation:`), `timeout`, and `on_error: fail | skip | warn`. A transient read or write retries through the one shared
retry; `on_error` `skip`/`warn` tolerate a failed step or output (`fail` is the default).
A key the Spark engine does not honour on a surface is an `unsupported-argument` error
naming the engine (for example `rate_limit` on an output).
Setting `checkpoint: true` on a step requires `runtime.spark.conf.spark.checkpoint.dir`; use a directory accessible
to the Spark cluster. With `persist` as well, the step is persisted, then checkpointed.

## Logging a step's rows

Any step may take DuckDBJob's `log: {message, level, max_rows}` with its meaning: each batch, one
`step NAME: MESSAGE` line per row of the step (`{column}` placeholders, a dotted `{a.b}` is one column, NULL as `null`; `level` `debug | info |
warning`, default `info`; `max_rows` 1..1000, default 20) on the `streamwright.steps` logger, then
`step NAME: ... and N more row(s)`; nothing for zero rows. Lines are redacted and cut to 1000 characters; a
placeholder the step's columns lack fails the step. Cost: the driver collects `limit(max_rows + 1)` (a Spark action)
and, only when that returns more than `max_rows`, runs `count()` (a second action); each computes the step again
unless it is persisted, so add `persist` to a costly step you log.

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
than `max_job_attempts` times, default 3, then stops; `block` stops until `streamwright state retry` or
`abandon`; an abandoned batch keeps its `job_key`, so use `retry` for keyed jobs) and
`apply_migrations`. The tables are the `sw_*`
tables of every engine. A run (and a state action) only checks them and fails (exit 1)
while a migration is pending: a deployment applies them with
`streamwright state migrate --url URL`; `run --apply-state-migrations` (forwarded to a
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
- A SparkJob's batches are looked after like any job's, from its state database alone: `streamwright
  state ACTION --url URL` (the `runtime.state` database: `sqlite:///...`, or the connection's
  `postgresql://` / `mysql://` URL; no job manifest, no config). `list [--job NAME] [--scope TEXT]
  [--status S] [--request-id ID] [--json]`, `show JOB_ID` (pinned inputs, tokens, rows, receipts,
  outbox), `retry JOB_ID` (resumes at the next run), `abandon JOB_ID`, `resend|skip JOB_ID`,
  `leases --job NAME`, `purge --job NAME --older-than 30d`, `migrate [--dry-run]`, `check`. retry,
  abandon, resend and skip take the batch's tokens (they fail while a run of its scope holds them).

## Commands

* `sw spark validate JOB [--overlay FILE] [--strict] [--analyze]`
* `sw spark plan JOB`
* `sw spark run JOB [--once | --drain | --available-now | --follow [--interval 10s]] [--job-key K]
  [--state-lock-policy fail|wait|prune] [--state-lock-timeout 5m] [--summary [FILE]]
  [--failed-job-policy abandon|retry|block] [--max-job-attempts N] [--apply-state-migrations]
  [--log-level LEVEL] [--log NAME=LEVEL] [--spark-log-level WARN]
  [--dry-run] [--detach]`: a run only checks the state tables (`streamwright state migrate --url
  URL` creates them; `--apply-state-migrations` lets the run do it);
  `--log-level`/`--log`/`--log-format` set streamwright's loggers (as on every command),
  `--spark-log-level` the JVM's; `--interval` needs `--follow`, `--detach` a remote platform;
  `--summary` writes the run's JSON summary (status `ok`, `skipped` or `failed`) on the driver.
* `sw spark lock JOB --spark VERSION [--check]` (the dependency lock file, checked by
  `run --lock F` / `--lock-required`; not `runtime.state.lock`): `--check` verifies the
  installed versions and that the lock still pins what the job's connectors need
* `streamwright state ACTION --url URL`: the job's batches (see State, scopes and locks)

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
