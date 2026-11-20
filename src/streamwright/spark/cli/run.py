#!/usr/bin/env python
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

"""sw spark run: submit a job to its platform, or run the core batch loop on the driver."""

import json
import logging
import os
from importlib import import_module, metadata

from streamwright.core.cli.jobs import batch_mode
from streamwright.core.cli.log_options import log_setup
from streamwright.core.config.inputs import InputError, parse_duration
from streamwright.core.jobs import BrokerCall, Runner
from streamwright.core.jobs.apply.delivery import Applier, apply_calls
from streamwright.core.jobs.grammar import to_internal
from streamwright.core.jobs.inputs import effective_config
from streamwright.core.jobs.inputs import secret_values as typed_secrets
from streamwright.core.jobs.locking import applies_migrations, check_key, lock_plan, resolve_failed_job_policy
from streamwright.core.jobs.log_labels import job_log_context
from streamwright.core.jobs.lookups import Lookups, has_lookups, lookup_optional
from streamwright.core.jobs.lookups.passes import run_passes
from streamwright.core.jobs.manifest import manifest_directory
from streamwright.core.jobs.refs import references
from streamwright.core.jobs.state import StateError
from streamwright.core.runtime import components, logs
from streamwright.core.runtime.logs.context import log_context, new_request_id
from streamwright.core.runtime.logs.lines import fields, utc_now
from streamwright.core.runtime.redact import Redactor
from streamwright.core.sdk.contract import InvalidConnectionError
from streamwright.spark.cli.common import InputsError, connections_of, load_plugins
from streamwright.spark.cli.inputs import config_values, provided_config, provided_secrets, secret_values
from streamwright.spark.cli.run_options import add_arguments, check_options, driver_flags
from streamwright.spark.cli.run_summary import summary, write_summary
from streamwright.spark.cli.state import open_store
from streamwright.spark.cli.submit_format import format_spark_submit
from streamwright.spark.contracts import LaunchError, LaunchFailureError
from streamwright.spark.io.messaging import open_broker
from streamwright.spark.job import JobError, check_job, load_job, resolve
from streamwright.spark.job.reader import resolve_job_location
from streamwright.spark.plan import build_plan, role_of
from streamwright.spark.runtime import lock as lockmod

__all__ = ["add_arguments", "apply_connectors", "run"]  # add_arguments: run_options

LOG = logging.getLogger("streamwright.spark")



def run(args):
    setup = log_setup(args)
    if setup is None:
        return 2
    try:
        return _run(args)
    except (JobError, InputError, InputsError, lockmod.LockError) as exc:     # logged while the run's logging is set up
        _error(exc)
        return 2
    except StateError as exc:                                          # e.g. a state migration is pending
        _error(exc, event="state_error")
        return 1
    finally:
        setup.close()


def _error(message, event="run_refused"):
    """A run's error: a log line (its format, handlers and redaction), not a bare print."""
    LOG.error("%s", message, extra=fields(event=event, status="failed"))


def _run(args):
    adapters, launchers, plugin_problems = load_plugins(args.allow_connector)
    job = load_job(args.job, args.overlay)
    problems, warnings = check_job(job, adapters, launchers)
    problems += plugin_problems
    for text in problems:
        _error(text)
    if problems:
        return 2
    for text in warnings:
        LOG.warning("%s", text, extra=fields(event="job_warning"))
    config = config_values(lookup_optional(job), args)    # lookup-bound inputs are filled on the driver
    platform = job["runtime"]["platform"]
    problem = logs.summary_problem(args.summary)
    problems = check_options(args, platform) + ([problem] if problem else [])
    for text in problems:
        _error(text)
    if problems:
        return 2
    lock = lockmod.load(args.lock) if args.lock else None
    if args.lock_required and lock is None:
        _error("--lock-required: no dependency lock file given (--lock FILE)")
        return 2
    if args.dry_run:
        try:
            _preview_dry_run(job, adapters, args, lock=lock, platform=platform, launcher=launchers[platform["type"]])
        except LaunchError as exc:
            _error(exc)
            return 2
        return 0
    if platform["type"] != "local" and not args.driver:
        placeholders = {name: "{{ secrets.%s }}" % name for name in ((job.get("spec") or {}).get("secrets") or {})}
        later = {name: "{{ config.%s }}" % name for name, value in config.items() if value is None}
        submission, problems = resolve(job, {**config, **later}, placeholders)
        if problems:
            for text in problems:
                _error(text)
            return 2
        return _submit(submission, args, lock, submission["runtime"]["platform"], launchers[platform["type"]])
    started, outcome, code, failure = utc_now(), {}, None, None
    try:
        with log_context(request_id=new_request_id()):       # one id for every line and batch of this run
            code = _drive(job, adapters, args, config, lock, outcome=outcome)
        return code
    except BaseException as exc:
        failure = exc
        raise
    finally:
        if args.summary:
            write_summary(args, summary(args, outcome, started, code, failure))


def _spark_conf(job, extra_conf):
    conf = dict((job.get("runtime") or {}).get("spark", {}).get("conf") or {})
    env = (job.get("runtime") or {}).get("env") or {}
    secret_values_in_env = [
        str(v) for k, v in env.items() if k.startswith("STREAMWRIGHT_SECRET_") and v and not references(v)
    ]
    for k, v in env.items():
        if k.startswith("STREAMWRIGHT_SECRET_") or (
            isinstance(v, str) and any(ref.startswith("secrets.") for ref in references(v))
        ):
            continue
        conf.setdefault("spark.yarn.appMasterEnv.%s" % k, str(v))
        conf.setdefault("spark.executorEnv.%s" % k, str(v))
    conf.update(item.split("=", 1) for item in extra_conf if "=" in item)
    redact = Redactor(secret_values_in_env)  # the one secret-matching rule (the forms logs mask)
    if any(redact(str(value)) != str(value) for value in conf.values()):
        raise LaunchError("resolved secrets cannot be copied into Spark conf")
    if any(
        key.startswith(("spark.yarn.appMasterEnv.STREAMWRIGHT_SECRET_", "spark.executorEnv.STREAMWRIGHT_SECRET_"))
        for key in conf
    ):
        raise LaunchError("secret environment variables cannot be put in Spark conf; use runtime.secrets")
    return conf


def _preview_dry_run(job, adapters, args, *, lock, platform, launcher):
    print("Execution Plan:")
    print(build_plan(to_internal(job), adapters).describe())
    print("\nLaunch Submission (platform: %s):" % platform["type"])
    conf = _spark_conf(job, args.conf)
    files = list(args.overlay) + ([args.config] if args.config else [])
    preview = launcher.preview(
        args.job, driver_flags(args), conf, lock or {}, platform=platform, lock_path=args.lock, files=files
    )
    if preview.get("target"):
        print("  Target Cluster: %s" % preview["target"])
    if preview.get("command"):
        print("  spark-submit command:\n  " + format_spark_submit(preview["command"], indent=4))
    if preview.get("step"):
        print("  EMR Step JSON:\n  " + json.dumps(preview["step"], indent=2).replace("\n", "\n  "))


def _submit(job, args, lock, platform, launcher):
    try:
        conf = _spark_conf(job, args.conf)
    except LaunchError as exc:
        _error(exc)
        return 2
    if getattr(args, "secrets", None):
        _error("--secrets is not delivered to cluster drivers; use runtime.secrets")
        return 2
    flags = driver_flags(args)
    env = (lock or {}).get("python_env") or {}
    archive = env.get("archive") if env.get("archive") and os.path.exists(env["archive"]) else None
    job_path = os.path.join(args.job, "job.yaml") if os.path.isdir(args.job) else resolve_job_location(args.job)
    try:
        lock_problems = launcher.check_lock(lock or {}, platform)
        for text in lock_problems:
            _error(text)
        if lock_problems:
            return 2
        files = list(args.overlay) + ([args.config] if args.config else [])
        names = [os.path.basename(path.rstrip("/")) for path in [job_path, args.lock, *files] if path]
        if len(names) != len(set(names)):
            raise LaunchError("job, lock, overlays and config must have distinct file names for cluster staging")
        return launcher.submit(
            job_path,
            flags,
            conf,
            lock or {},
            detach=args.detach,
            lock_path=args.lock,
            env_archive=archive,
            platform=platform,
            files=files,
        )
    except LaunchFailureError as exc:
        _error(exc)
        return 1
    except LaunchError as exc:
        _error(exc)
        return 2


def _verify_lock(lock):
    if lock is None:
        return []
    mismatches = lockmod.check_lock(lock, metadata.version("pyspark"))
    if not mismatches:
        os.environ["STREAMWRIGHT_LOCK_SHA"] = lockmod.fingerprint(lock)
    return mismatches


def _configure_driver_conf(resolved, args, secrets):
    conf = _spark_conf(resolved, getattr(args, "conf", None) or [])
    redact = Redactor(secrets.values())  # the one secret-matching rule (the forms logs mask)
    if any(redact(str(value)) != str(value) for value in conf.values()):
        raise LaunchError("resolved secrets cannot be copied into Spark conf")
    resolved.setdefault("runtime", {}).setdefault("spark", {})["conf"] = conf


def _broker_calls(resolved, connections, adapters):
    brokers = {}

    def broker_for(name):
        if name not in brokers:
            conn = connections[name]
            adapter = adapters[conn.provider]
            brokers[name] = open_broker(adapter, conn.auth)
        return brokers[name]

    consumer = None
    publishers = {}
    for step in resolved.get("steps") or []:
        if role_of(resolved, step, adapters) == "consume":
            consumer = BrokerCall(broker_for(step["connection"]), step["arguments"])
    for output in resolved.get("outputs") or []:
        if role_of(resolved, output, adapters) == "publish":
            publishers[output["name"]] = BrokerCall(broker_for(output["connection"]), output["arguments"])
    return consumer, publishers


def _drive(job, adapters, args, config, lock, *, outcome):
    """On the driver: the run, or with spec.lookups its inputs looked up first (once per runtime.fan_out item)."""
    if not has_lookups(job):
        secrets = secret_values(job, args)
        outcome["redact"] = Redactor(secrets.values())
        with job_log_context(job, config):
            return _drive_one(job, adapters, args, config, secrets, lock=lock, outcome=outcome)
    redact = outcome["redact"] = Redactor()

    def enrol(value):
        if value not in (None, "") and not isinstance(value, bool):
            redact.add(value)

    lookups = Lookups(job, provided_config(args), provided_secrets(job, args), enrol=enrol,
                      allow_exec=args.allow_lookup_exec, base_dir=manifest_directory(args.job))
    if lookups.fan_out and int(args.fan_out_concurrency or lookups.fan_out["concurrency"]) > 1:
        LOG.warning("fan_out: a SparkJob runs its items one at a time (concurrency 1)",
                    extra=fields(event="fan_out_concurrency"))

    def run_one(label, pass_config, pass_secrets):
        secrets = typed_secrets(job, pass_secrets)
        for value in secrets.values():
            enrol(value)
        part = outcome if lookups.fan_out is None else {"redact": redact}
        with job_log_context(job, pass_config):
            return _drive_one(job, adapters, args, effective_config(job, pass_config), secrets, lock=lock,
                              outcome=part), part

    outcomes = run_passes(lookups, run_one, concurrency=1, on_failure=args.fan_out_on_failure)
    if lookups.fan_out is None:
        return outcomes[0].exit_code
    outcome.update(passes=outcomes, fan_out=lookups.fan_out, job=job["name"])
    for item in outcomes:
        runner = (item.result or {}).get("runner")
        error = item.error or (runner.error if runner else None)
        LOG.log(logging.INFO if item.status == "ok" else logging.WARNING, "%s %s %s%s", lookups.fan_out["as"],
                item.label, item.status.replace("_", " "), ": %s" % redact(str(error)) if error else "",
                extra=fields(event="pass_end", status=item.status))
    return 1 if any(item.status != "ok" for item in outcomes) else 0


def apply_connectors(adapters):
    """
    The connectors an output may call as an apply output: the installed ones without a Spark adapter (an output on a
    connection whose provider has one is that adapter's write, e.g. `database.write`), as the job's checks decide.
    """
    return {name for name in components.available() if name not in adapters}


def _drive_one(job, adapters, args, config, secrets, *, lock, outcome):
    """On the driver (local, or inside the cluster): verify, connect, run the batch loop. Needs Spark."""
    executor_type = import_module("streamwright.spark.engine.executor").SparkExecutor
    session_module = import_module("streamwright.spark.engine.session")
    job = to_internal(job)                       # the engine reads the internal shape (core jobs.grammar)
    resolved, problems = resolve(job, config, secrets)
    for text in problems:
        _error(text)
    if problems:
        return 2
    try:
        state_lock = lock_plan(job, resolved, config, policy=getattr(args, "state_lock_policy", None),
                         timeout=getattr(args, "state_lock_timeout", None))
        failed_jobs = resolve_failed_job_policy(resolved, getattr(args, "failed_job_policy", None),
                                                getattr(args, "max_job_attempts", None))
        if _job_key(args, resolved) is not None:
            check_key("the job key", _job_key(args, resolved))
    except InputError as exc:
        _error(exc)
        return 2
    for key, value in ((resolved.get("runtime") or {}).get("env") or {}).items():
        os.environ[key] = str(value)
    try:
        connections = connections_of(resolved, adapters=adapters, declared=job, config=config, secrets=secrets)
    except InvalidConnectionError as exc:
        _error(exc)
        return 2
    if batch_mode(args) != "once" and args.max_messages and not any(role_of(resolved, step, adapters) == "consume"
                                              for step in resolved.get("steps") or ()):
        _error("--%s needs a job that consumes a broker (a step with method: consume): without one every batch "
            "is new work and the run would not end" % batch_mode(args).replace("_", "-"))
        return 2
    mismatches = _verify_lock(lock)
    for text in mismatches:
        _error("lock: " + text)
    if mismatches:
        return 2
    store = open_store(resolved, connections, adapters, manifest_directory(args.job))
    store.prepare(migrate=applies_migrations(resolved, args.apply_state_migrations))
    try:
        _configure_driver_conf(resolved, args, secrets)
    except LaunchError as exc:
        _error(exc)
        return 2
    spark = session_module.build_session(
        resolved,
        connections,
        adapters,
        local=resolved["runtime"]["platform"]["type"] == "local",
        log_level=getattr(args, "spark_log_level", None),
    )
    app_id, driver_host = session_module.resolve_spark_metadata(spark)
    def prepare(inputs):
        """The executor of a batch, from its pinned config values (a lock input narrowed to its items)."""
        pinned = effective_config(job, inputs["config"]) if inputs else config
        pinned_job, problems = resolve(job, pinned, secrets) if inputs else (resolved, [])
        if problems:
            raise LaunchError("; ".join(problems))
        return executor_type(
            spark, pinned_job, connections, adapters, config=pinned, limits=pinned_job["runtime"].get("limits"),
            state=store, declared_connections=(job.get("connections") or {}), secrets=secrets,
            allowed_connectors=getattr(args, "allow_connector", None), apply_outputs=applier.calls, redact=redact,
            job_location=args.job,
        )

    consumer, publishers = _broker_calls(resolved, connections, adapters)
    redact = Redactor(secrets.values())          # one instance: tokens a connector obtains are masked everywhere
    applier = Applier(apply_calls(resolved, job.get("connections") or {}, apply_connectors(adapters)),
                      config=config, secrets=secrets, redact=redact)

    mode = batch_mode(args)
    runner = Runner(
        resolved["name"],
        store,
        None,
        consumer,
        publishers,
        redact=redact,
        lease_seconds=(resolved["runtime"].get("state") or {}).get("ttl_seconds", 300),
        engine_metadata={"app_id": app_id, "driver_host": driver_host},
        scope=state_lock.scope,
        lock=state_lock,
        prepare=prepare,
        failed_jobs=failed_jobs,
        applier=applier,
    )
    outcome.update(runner=runner, store=store, job=resolved["name"])
    interval = parse_duration(args.interval or "10s").total_seconds()
    try:
        return runner.run(mode, interval, args.max_messages, _job_key(args, resolved))
    finally:
        applier.close()


def _job_key(args, job):
    """--job-key, else runtime.state.job_key."""
    if args.job_key is not None:
        return args.job_key
    return ((job.get("runtime") or {}).get("state") or {}).get("job_key")
