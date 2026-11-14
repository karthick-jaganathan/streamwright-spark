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

"""sw spark lock: write or check the version lock."""

import os
from importlib import metadata

from streamwright.core.runtime import components, plugins
from streamwright.spark.cli.common import err, load_plugins
from streamwright.spark.job import check_job, load_job
from streamwright.spark.launchers.common import sha256_file
from streamwright.spark.runtime import lock as lockmod

__all__ = ["add_arguments", "run"]


def add_arguments(parser):
    parser.add_argument("--spark", metavar="VERSION",
                        help="the Spark version the job runs on, e.g. 3.5.3: needed to write a lock; --check compares "
                             "the lock with it (default: the installed pyspark)")
    parser.add_argument(
        "--out", metavar="FILE",
        help="the lock file to write (--check: to read); default runtime.artifacts.lock or <name>.lock.json"
    )
    parser.add_argument("--emr-release", metavar="LABEL",
                        help="EMR release label (e.g. emr-7.2.0) to record in the lock file (--check: to compare)")
    parser.add_argument(
        "--env-archive", metavar="FILE",
        help="a packaged virtual environment (.tar.gz) for remote executors, recorded with its sha256 (not with "
             "--check)"
    )
    parser.add_argument(
        "--jar",
        action="append",
        default=[],
        metavar="GROUP:ARTIFACT=VERSION",
        help="a JAR's version (repeatable): for a jar a connector needs without a default version, or to replace "
             "its default",
    )
    parser.add_argument("--check", action="store_true",
                        help="verify the existing lock file (--out, or the default) instead of writing one: the "
                             "installed Spark, Python and package versions, the EMR release (--emr-release), and that "
                             "it pins the packages and jars the job needs now (with --jar)")


def _problems(args):
    problems = []
    if args.check and args.env_archive:
        problems.append("--env-archive is recorded when writing a lock, not with --check")
    if not args.check and not args.spark:
        problems.append("--spark VERSION is needed to write a lock (the Spark version the job runs on)")
    for item in args.jar:
        coordinates, _, version = item.partition("=")
        if coordinates.count(":") != 1 or not version or not all(coordinates.split(":")):
            problems.append("--jar %r: give GROUP:ARTIFACT=VERSION" % item)
    return problems


def run(args):
    problems = _problems(args)
    for text in problems:
        err("error: " + text)
    if problems:
        return 2
    spark = args.spark or metadata.version("pyspark")
    adapters, launchers, _ = load_plugins(args.allow_connector)
    job = load_job(args.job, args.overlay)
    problems, _ = check_job(job, adapters, launchers)
    if problems:
        for text in problems:
            err("error: " + text)
        return 2
    used = sorted({c["provider"] for c in job["connections"].values()})
    distributions = {entry["name"]: entry.get("distribution") for entry in plugins.discover(plugins.CONNECTORS)}
    driver_only = [distributions.get(p) or components.package_name(p) for p in used if p not in adapters]
    jars = dict(item.split("=", 1) for item in args.jar)
    try:
        python_env = None
        if args.env_archive:
            python_env = {"archive": os.path.basename(args.env_archive), "sha256": sha256_file(args.env_archive)}
        lock = lockmod.build_lock(
            job["name"],
            [adapters[p] for p in used if p in adapters],
            spark,
            extra_jars=jars,
            driver_packages=driver_only,
            emr_release=args.emr_release,
            python_env=python_env,
        )
    except lockmod.LockError as exc:
        err("error: %s" % exc)
        return 2
    out = args.out or ((job.get("runtime") or {}).get("artifacts") or {}).get("lock") or "%s.lock.json" % job["name"]
    if args.check:
        existing = lockmod.load(out)
        problems = lockmod.check_lock(existing, spark, emr_release=args.emr_release)
        problems += lockmod.lock_drift(existing, lock)
        if not problems:
            print("%s matches the job and the installed versions" % out)
        for text in problems:
            err("lock: " + text)
        return 2 if problems else 0
    lockmod.dump(lock, out)
    print("wrote %s (fingerprint %s)" % (out, lockmod.fingerprint(lock)[:12]))
    return 0
