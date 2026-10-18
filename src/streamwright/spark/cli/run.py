"""sw spark run: submit a job to its platform, or (on the driver) run the batch loop. PROPOSED."""

import datetime
import json
import os
import sys

from streamwright.spark.cli.common import connections_of, err, load_plugins
from streamwright.spark.cli.inputs import config_values, secret_values
from streamwright.spark.cli.state import open_store
from streamwright.spark.contracts import LaunchError, Role
from streamwright.spark.job import check_job, load_job, resolve
from streamwright.spark.plan import build_plan, role_of
from streamwright.spark.runtime import lock as lockmod


__all__ = ["add_arguments", "run"]


def add_arguments(parser):
    parser.add_argument("--secrets", help="path to a secrets YAML file or JSON string of secret values")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="one batch (default)")
    mode.add_argument("--drain", action="store_true", help="batches until the source is empty")
    mode.add_argument("--follow", action="store_true", help="keep polling until interrupted")
    parser.add_argument("--interval", default="10s", help="polling interval when running with --follow (default: 10s)")
    parser.add_argument("--max-messages", type=int, default=500, help="maximum messages per batch (0: recover and relay only)")
    parser.add_argument("--run-key", help="idempotency key for jobs without a broker")
    parser.add_argument("--conf", action="append", default=[], metavar="K=V", help="Spark configuration override (e.g. --conf spark.executor.memory=4g)")
    parser.add_argument("--log-level", choices=["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"],
                        type=str.upper, help="Spark logging level (e.g. --log-level ERROR or WARN)")
    parser.add_argument("--lock", help="path to the lock file to verify dependencies and versions against")
    parser.add_argument("--lock-required", action="store_true", help="require a valid lock file before running")
    parser.add_argument("--no-init-state", action="store_true",
                        help="do not create the bookkeeping tables (they must exist: `sw spark state init JOB`)")
    parser.add_argument("--dry-run", action="store_true", help="resolve the job and print the launch submission without executing")
    parser.add_argument("--detach", action="store_true", help="submit the job to the cluster launcher and exit without waiting")
    parser.add_argument("--summary", action="store_true", help="print a summary of the executed batch upon completion")
    parser.add_argument("--driver", action="store_true", help="internal: run on the cluster driver")


def _seconds(text):
    import re
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
        _preview_dry_run(job, adapters, args, lock, platform)
        return 0
    if platform["type"] != "local" and not args.driver:
        return _submit(job, args, lock, platform, launchers[platform["type"]])
    return _drive(job, adapters, args, config, lock)


def format_spark_submit(argv, indent=4):
    """Pretty prints a spark-submit argument list grouping flags with their values into a clean multiline command."""
    lines = []
    i = 0
    if argv and argv[0] == "spark-submit":
        lines.append("spark-submit")
        i = 1

    BOOL_FLAGS = {
        "--once", "--drain", "--follow", "--no-init-state", "--dry-run",
        "--detach", "--summary", "--driver", "--supervise", "--verbose"
    }

    prefix = " " * indent
    while i < len(argv):
        arg = str(argv[i])
        if arg.startswith("-"):
            if arg in BOOL_FLAGS or (i + 1 < len(argv) and str(argv[i + 1]).startswith("-")):
                lines.append("%s%s" % (prefix, arg))
                i += 1
            elif i + 1 < len(argv):
                val = str(argv[i + 1])
                if " " in val and not (val.startswith('"') and val.endswith('"')):
                    val = '"%s"' % val
                lines.append("%s%s %s" % (prefix, arg, val))
                i += 2
            else:
                lines.append("%s%s" % (prefix, arg))
                i += 1
        else:
            val = arg
            if " " in val and not (val.startswith('"') and val.endswith('"')):
                val = '"%s"' % val
            lines.append("%s%s" % (prefix, val))
            i += 1

    return " \\\n".join(lines)


def _preview_dry_run(job, adapters, args, lock, platform):
    print("Execution Plan:")
    print(build_plan(job, adapters).describe())
    print("\nLaunch Submission (platform: %s):" % platform["type"])
    conf = dict(job["runtime"]["spark"].get("conf") or {})
    conf.update(item.split("=", 1) for item in args.conf if "=" in item)
    flags = []
    for item in args.set:
        flags += ["--set", item]
    if args.config:
        flags += ["--config", os.path.basename(args.config)]
    flags += ["--follow"] if args.follow else ["--drain"] if args.drain else ["--once"]
    flags += ["--interval", args.interval, "--max-messages", str(args.max_messages)]
    if args.run_key:
        flags += ["--run-key", args.run_key]
    if args.no_init_state:
        flags += ["--no-init-state"]
    if getattr(args, "log_level", None):
        flags += ["--log-level", args.log_level]

    if platform["type"] == "local":
        print("  Driver mode: in-process local SparkSession (local[*])")
        local_cmd = ["spark-submit", "--master", "local[*]"]
        for k, v in sorted(conf.items()):
            local_cmd += ["--conf", "%s=%s" % (k, v)]
        local_cmd.append(os.path.basename(args.job))
        local_cmd += flags
        print("\n  Equivalent spark-submit command:")
        print("  " + format_spark_submit(local_cmd, indent=4))
    elif platform["type"] == "standalone":
        from streamwright.spark.launchers.standalone import standalone_args
        cmd = standalone_args(platform.get("master", "spark://<master>:7077"), conf, lock or {}, args.job, flags)
        print("  spark-submit command:")
        print("  " + format_spark_submit(cmd, indent=4))
    elif platform["type"] == "emr_ec2":
        from streamwright.spark.launchers.emr_ec2 import emr_step_args
        env = (lock or {}).get("python_env") or {}
        archive = env.get("archive")
        staging = platform.get("staging", "s3://<staging-bucket>/staging/")
        if not staging.endswith("/"):
            staging += "/"
        staged = {
            "entry": "%s<sha256>/entrypoint.py" % staging,
            "job": "%s<sha256>/%s" % (staging, os.path.basename(args.job)),
            "lock": "%s<sha256>/%s" % (staging, os.path.basename(args.lock or "job.lock.json")),
            "env": ("%s<sha256>/%s" % (staging, archive)) if archive else None,
        }
        driver_argv = [os.path.basename(args.job), "--lock", os.path.basename(args.lock or "job.lock.json")] + flags
        cmd = emr_step_args(conf, lock or {}, staged, driver_argv, platform.get("deploy_mode", "cluster"))
        step = {
            "Name": "streamwright-%s" % job.get("name", "job"),
            "ActionOnFailure": (platform.get("step") or {}).get("action_on_failure", "CONTINUE"),
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": cmd,
            },
        }
        print("  Target Cluster: %s (region: %s)" % (platform.get("cluster", {}).get("id", "<cluster-id>"), platform.get("region", "<region>")))
        print("\n  spark-submit command:")
        print("  " + format_spark_submit(cmd, indent=4))
        print("\n  EMR Step JSON:")
        print("  " + json.dumps(step, indent=2).replace("\n", "\n  "))
    else:
        print("  Platform launcher: %s" % platform["type"])


def _submit(job, args, lock, platform, launcher):
    conf = dict(job["runtime"]["spark"].get("conf") or {})
    conf.update(item.split("=", 1) for item in args.conf if "=" in item)
    flags = []
    for item in args.set:
        flags += ["--set", item]
    if args.config:
        flags += ["--config", os.path.basename(args.config)]
    flags += ["--follow"] if args.follow else ["--drain"] if args.drain else ["--once"]
    flags += ["--interval", args.interval, "--max-messages", str(args.max_messages)]
    if args.run_key:
        flags += ["--run-key", args.run_key]
    if args.no_init_state:
        flags += ["--no-init-state"]
    if getattr(args, "log_level", None):
        flags += ["--log-level", args.log_level]
    env = (lock or {}).get("python_env") or {}
    archive = env.get("archive") if env.get("archive") and os.path.exists(env["archive"]) else None
    try:
        lock_problems = launcher.check_lock(lock or {}, platform)
        for text in lock_problems:
            err("error: " + text)
        if lock_problems:
            return 2
        return launcher.submit(args.job, flags, conf, lock or {}, args.detach, lock_path=args.lock,
                               env_archive=archive, platform=platform)
    except LaunchError as exc:
        err("error: %s" % exc)
        return 2


def _drive(job, adapters, args, config, lock):
    """On the driver (local, or inside the cluster): verify, connect, run the batch loop. Needs Spark."""
    from streamwright.spark.engine.executor import SparkExecutor
    from streamwright.spark.engine.session import build_session
    from streamwright.spark.runtime.runner import Runner

    secrets = secret_values(job, args)
    resolved, problems = resolve(job, config, secrets)
    if problems:
        for text in problems:
            err("error: " + text)
        return 2
    connections = connections_of(resolved)
    import pyspark
    if lock is not None:
        mismatches = lockmod.check_lock(lock, pyspark.__version__)
        if mismatches:
            for text in mismatches:
                err("error: lock: " + text)
            return 2
        os.environ["STREAMWRIGHT_LOCK_SHA"] = lockmod.fingerprint(lock)
    store = open_store(resolved, connections, adapters)
    if not args.no_init_state:
        store.init()                    # idempotent CREATE ... IF NOT EXISTS; skip with --no-init-state (no DDL rights)
    if getattr(args, "conf", None):
        spark_conf = resolved["runtime"]["spark"].setdefault("conf", {})
        for item in args.conf:
            if "=" in item:
                k, v = item.split("=", 1)
                spark_conf[k] = v
    spark = build_session(resolved, connections, adapters,
                          local=resolved["runtime"]["platform"]["type"] == "local",
                          log_level=getattr(args, "log_level", None))
    from streamwright.spark.engine.session import resolve_spark_metadata
    app_id, driver_host = resolve_spark_metadata(spark)
    executor = SparkExecutor(spark, resolved, connections, adapters, config, resolved["runtime"].get("limits"))
    brokers, consume, publishers = {}, None, {}

    def broker_for(name):
        if name not in brokers:
            conn = connections[name]
            brokers[name] = adapters[conn.provider].broker(conn.auth)
        return brokers[name]

    consumer = None
    for step in resolved.get("steps") or []:
        if role_of(resolved, step, adapters) == Role.CONSUME:
            consumer = broker_for(step["connection"])
            consume = {"arguments": step["arguments"]}
    for out in resolved.get("outputs") or []:
        if role_of(resolved, out, adapters) == Role.PUBLISH:
            publishers[out["name"]] = (broker_for(out["connection"]), out["arguments"])

    def log(event, **fields):
        print(json.dumps(dict(fields, event=event, ts=datetime.datetime.now(datetime.timezone.utc).isoformat()),
                         default=str), file=sys.stderr)

    mode = "follow" if args.follow else "drain" if args.drain else "once"
    owner = f"{driver_host}:{app_id}:{os.getpid()}" if app_id else f"{driver_host}:{os.getpid()}"
    runner = Runner(resolved["name"], store, executor, consumer, consume, publishers, [v for v in secrets.values() if v],
                    owner=owner, log=log, app_id=app_id, driver_host=driver_host)
    return runner.run(mode, _seconds(args.interval), args.max_messages, args.run_key)
