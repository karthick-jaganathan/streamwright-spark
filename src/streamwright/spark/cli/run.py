"""sw spark run: submit a job to its platform, or run the core batch loop on the driver."""

import datetime
import json
import os
import re
import sys
from importlib import import_module, metadata

from streamwright.core.jobs import BrokerCall, Runner
from streamwright.core.jobs.manifest import manifest_directory
from streamwright.core.jobs.refs import references
from streamwright.core.runtime.redact import Redactor
from streamwright.core.sdk.contract import InvalidConnectionError
from streamwright.spark.cli.common import connections_of, err, load_plugins
from streamwright.spark.cli.inputs import config_values, secret_values
from streamwright.spark.cli.state import open_store
from streamwright.spark.cli.submit_format import format_spark_submit
from streamwright.spark.contracts import LaunchError, LaunchFailureError
from streamwright.spark.io.messaging import open_broker
from streamwright.spark.job import check_job, load_job, resolve
from streamwright.spark.job.reader import resolve_job_location
from streamwright.spark.plan import build_plan, role_of
from streamwright.spark.runtime import lock as lockmod

__all__ = ["add_arguments", "run"]


def add_arguments(parser):
    parser.add_argument("--secrets", help="path to a secrets YAML file or JSON string of secret values")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="one batch (default)")
    mode.add_argument("--drain", action="store_true", help="batches until the source is empty")
    mode.add_argument(
        "--available-now",
        action="store_true",
        help="batches until messages available at launch are processed (Trigger.AvailableNow)",
    )
    mode.add_argument("--follow", action="store_true", help="keep polling until interrupted")
    parser.add_argument("--interval", default="10s", help="polling interval when running with --follow (default: 10s)")
    parser.add_argument(
        "--max-messages", type=int, default=500, help="maximum messages per batch (0: recover and relay only)"
    )
    parser.add_argument("--run-key", help="idempotency key for jobs without a broker")
    parser.add_argument(
        "--conf",
        action="append",
        default=[],
        metavar="K=V",
        help="Spark configuration override (e.g. --conf spark.executor.memory=4g)",
    )
    parser.add_argument(
        "--log-level",
        choices=["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"],
        type=str.upper,
        help="Spark logging level (e.g. --log-level ERROR or WARN)",
    )
    parser.add_argument("--lock", help="path to the lock file to verify dependencies and versions against")
    parser.add_argument("--lock-required", action="store_true", help="require a valid lock file before running")
    parser.add_argument(
        "--no-init-state",
        action="store_true",
        help="do not create the bookkeeping tables (they must exist: `sw spark state init JOB`)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="resolve the job and print the launch submission without executing"
    )
    parser.add_argument(
        "--detach", action="store_true", help="submit the job to the cluster launcher and exit without waiting"
    )
    parser.add_argument("--summary", action="store_true", help="print a summary of the executed batch upon completion")
    parser.add_argument("--driver", action="store_true", help="internal: run on the cluster driver")


def _seconds(text):
    match = re.match(r"^([0-9]+)([smh])$", str(text))
    if not match:
        raise ValueError("invalid duration %r (expected e.g. 10s, 5m)" % (text,))
    return int(match.group(1)) * {"s": 1, "m": 60, "h": 3600}[match.group(2)]


def run(args):
    adapters, launchers, plugin_problems = load_plugins(args.allow_connector)
    job = load_job(args.job, args.overlay)
    problems, warnings = check_job(job, adapters, launchers)
    problems += plugin_problems
    for text in problems:
        err("error: " + text)
    if problems:
        return 2
    for text in warnings:
        err("warning: " + text)
    config = config_values(job, args)
    platform = job["runtime"]["platform"]
    lock = lockmod.load(args.lock) if args.lock else None
    if args.lock_required and lock is None and platform["type"] != "local":
        err("error: --lock-required: no lock given")
        return 2
    if args.dry_run:
        try:
            _preview_dry_run(job, adapters, args, lock=lock, platform=platform, launcher=launchers[platform["type"]])
        except LaunchError as exc:
            err("error: %s" % exc)
            return 2
        return 0
    if platform["type"] != "local" and not args.driver:
        placeholders = {name: "{{ secrets.%s }}" % name for name in ((job.get("spec") or {}).get("secrets") or {})}
        submission, problems = resolve(job, config, placeholders)
        if problems:
            for text in problems:
                err("error: " + text)
            return 2
        return _submit(submission, args, lock, submission["runtime"]["platform"], launchers[platform["type"]])
    return _drive(job, adapters, args, config, lock)


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


def _driver_flags(args):
    flags = []
    for path in args.overlay:
        flags += ["--overlay", os.path.basename(path)]
    for item in args.set:
        flags += ["--set", item]
    if args.config:
        flags += ["--config", os.path.basename(args.config)]
    flags += (
        ["--follow"]
        if args.follow
        else ["--available-now"]
        if getattr(args, "available_now", False)
        else ["--drain"]
        if args.drain
        else ["--once"]
    )
    flags += ["--interval", args.interval, "--max-messages", str(args.max_messages)]
    if args.run_key:
        flags += ["--run-key", args.run_key]
    if args.no_init_state:
        flags += ["--no-init-state"]
    if getattr(args, "log_level", None):
        flags += ["--log-level", args.log_level]
    return flags


def _preview_dry_run(job, adapters, args, *, lock, platform, launcher):
    print("Execution Plan:")
    print(build_plan(job, adapters).describe())
    print("\nLaunch Submission (platform: %s):" % platform["type"])
    conf = _spark_conf(job, args.conf)
    files = list(args.overlay) + ([args.config] if args.config else [])
    preview = launcher.preview(
        args.job, _driver_flags(args), conf, lock or {}, platform=platform, lock_path=args.lock, files=files
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
        err("error: %s" % exc)
        return 2
    if getattr(args, "secrets", None):
        err("error: --secrets is not delivered to cluster drivers; use runtime.secrets")
        return 2
    flags = _driver_flags(args)
    env = (lock or {}).get("python_env") or {}
    archive = env.get("archive") if env.get("archive") and os.path.exists(env["archive"]) else None
    job_path = os.path.join(args.job, "job.yaml") if os.path.isdir(args.job) else resolve_job_location(args.job)
    try:
        lock_problems = launcher.check_lock(lock or {}, platform)
        for text in lock_problems:
            err("error: " + text)
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
        err("error: %s" % exc)
        return 1
    except LaunchError as exc:
        err("error: %s" % exc)
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


def _drive(job, adapters, args, config, lock):
    """On the driver (local, or inside the cluster): verify, connect, run the batch loop. Needs Spark."""
    executor_type = import_module("streamwright.spark.engine.executor").SparkExecutor
    session_module = import_module("streamwright.spark.engine.session")

    secrets = secret_values(job, args)
    resolved, problems = resolve(job, config, secrets)
    for text in problems:
        err("error: " + text)
    if problems:
        return 2
    for key, value in ((resolved.get("runtime") or {}).get("env") or {}).items():
        os.environ[key] = str(value)
    try:
        connections = connections_of(resolved, adapters=adapters, declared=job, config=config, secrets=secrets)
    except InvalidConnectionError as exc:
        err("error: " + str(exc))
        return 2
    mismatches = _verify_lock(lock)
    for text in mismatches:
        err("error: lock: " + text)
    if mismatches:
        return 2
    store = open_store(resolved, connections, adapters, manifest_directory(args.job))
    if not args.no_init_state:
        store.initialize()
    try:
        _configure_driver_conf(resolved, args, secrets)
    except LaunchError as exc:
        err("error: " + str(exc))
        return 2
    spark = session_module.build_session(
        resolved,
        connections,
        adapters,
        local=resolved["runtime"]["platform"]["type"] == "local",
        log_level=getattr(args, "log_level", None),
    )
    app_id, driver_host = session_module.resolve_spark_metadata(spark)
    executor = executor_type(
        spark, resolved, connections, adapters, config=config, limits=resolved["runtime"].get("limits"),
        state=store, declared_connections=(job.get("connections") or {}), secrets=secrets,
        allowed_connectors=getattr(args, "allow_connector", None),
    )
    consumer, publishers = _broker_calls(resolved, connections, adapters)

    def log(event, **fields):
        print(
            json.dumps(
                dict(fields, event=event, ts=datetime.datetime.now(datetime.UTC).isoformat()), default=str
            ),
            file=sys.stderr,
        )

    mode = (
        "follow"
        if args.follow
        else "available_now"
        if getattr(args, "available_now", False)
        else "drain"
        if args.drain
        else "once"
    )
    runner = Runner(
        resolved["name"],
        store,
        executor,
        consumer,
        publishers,
        redact=Redactor(secrets.values()),
        log=log,
        engine_metadata={"app_id": app_id, "driver_host": driver_host},
    )
    return runner.run(mode, _seconds(args.interval), args.max_messages, args.run_key)
