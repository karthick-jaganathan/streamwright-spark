"""sw spark lock: write or check the version lock. PROPOSED."""

import os

from streamwright.spark.cli.common import err, load_plugins
from streamwright.spark.job import check_job, load_job
from streamwright.spark.runtime import lock as lockmod


__all__ = ["add_arguments", "run"]


def add_arguments(parser):
    parser.add_argument("--spark", required=True, help="the Spark version the job runs on, e.g. 3.5.3")
    parser.add_argument("--out", help="output path for the generated lock file (defaults to runtime.artifacts.lock or <name>.lock.json)")
    parser.add_argument("--emr-release", help="EMR release label (e.g. emr-7.2.0) to record in the lock file")
    parser.add_argument("--env-archive", help="path to packaged virtual environment archive (.tar.gz) for remote executors")
    parser.add_argument("--jar", action="append", default=[], metavar="GROUP:ARTIFACT=VERSION",
                        help="JAR dependency override in GROUP:ARTIFACT=VERSION format")
    parser.add_argument("--check", action="store_true", help="verify an existing lock instead of writing one")


def run(args):
    adapters, launchers, _ = load_plugins(args.allow_connector)
    job = load_job(args.job, args.overlay)
    problems, _ = check_job(job, adapters, launchers)
    if problems:
        for text in problems:
            err("error: " + text)
        return 2
    used = sorted(set(c["provider"] for c in job["connections"].values()))
    jars = dict(item.split("=", 1) for item in args.jar if "=" in item)
    try:
        python_env = None
        if args.env_archive:
            from streamwright.spark.launchers.common import sha256_file
            python_env = {"archive": os.path.basename(args.env_archive), "sha256": sha256_file(args.env_archive)}
        lock = lockmod.build_lock(job["name"], [adapters[p] for p in used], args.spark, jars, args.emr_release, python_env)
    except lockmod.LockError as exc:
        err("error: %s" % exc)
        return 2
    out = args.out or ((job.get("runtime") or {}).get("artifacts") or {}).get("lock") or "%s.lock.json" % job["name"]
    if args.check:
        problems = lockmod.check_lock(lockmod.load(out), args.spark)
        for text in problems:
            err("lock: " + text)
        return 2 if problems else 0
    lockmod.dump(lock, out)
    print("wrote %s (fingerprint %s)" % (out, lockmod.fingerprint(lock)[:12]))
    return 0
