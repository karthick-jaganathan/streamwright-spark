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

"""The options of `sw spark run`: their definitions, the checks of those that do not apply, the ones a driver gets."""

import argparse
import logging
import os

from streamwright.core.cli.jobs import (
    JOB_HELP,
    add_apply_migrations_argument,
    add_batch_arguments,
    add_failed_job_arguments,
    add_fan_out_arguments,
    add_job_key_argument,
    add_lock_arguments,
    add_renamed_option,
    batch_problems,
)
from streamwright.core.cli.log_options import add_log_options
from streamwright.core.runtime import logs
from streamwright.spark.contracts import LAUNCHER_OWNED_CONF

__all__ = ["add_arguments", "check_options", "driver_flags"]


def add_arguments(parser):
    parser.add_argument("--secrets", metavar="FILE", help=JOB_HELP["secrets"])
    add_batch_arguments(parser)
    add_job_key_argument(parser)
    add_lock_arguments(parser)
    add_failed_job_arguments(parser)
    add_fan_out_arguments(parser)
    parser.add_argument(
        "--conf",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="a Spark configuration value (repeatable), over runtime.spark.conf, e.g. spark.executor.memory=4g; "
             "keys need the spark. prefix, and the launcher's own (spark.master, spark.submit.deployMode, ...) are "
             "refused",
    )
    parser.add_argument(
        "--spark-log-level",
        choices=["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"],
        type=str.upper,
        metavar="LEVEL",
        help="the Spark (JVM) log level: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN (default: "
             "runtime.spark.conf spark.log.level, else Spark's); --log-level sets streamwright's loggers",
    )
    add_log_options(parser)
    parser.add_argument("--lock", metavar="FILE",
                        help="the dependency lock file (written by sw spark lock) to verify package and Spark versions "
                             "against; not runtime.state.lock (see --state-lock-policy)")
    parser.add_argument("--lock-required", action="store_true",
                        help="refuse to run without a valid dependency lock file (--lock)")
    add_apply_migrations_argument(parser)
    add_renamed_option(parser, "--no-init-state",
                       message="--no-init-state is not needed: a run no longer creates or upgrades the state tables "
                               "(--apply-state-migrations makes it)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="validate and resolve the job, then print its plan and the submission it would launch (the spark-submit "
             "command, or the EMR step); run nothing"
    )
    parser.add_argument(
        "--detach", action="store_true",
        help="remote platforms: submit the job to the cluster launcher and exit without waiting"
    )
    parser.add_argument("--summary", nargs="?", const=logs.LOG_SUMMARY, metavar="FILE",
                        help="also write the run's summary as JSON to FILE (on the driver), whatever the outcome: "
                             "status (ok, skipped when the lock policy pruned all its work, or failed), times, its "
                             "batches' job ids, each output's rows and files, and the error; a bare --summary logs it "
                             "as one INFO line of compact JSON (event run_summary)")
    parser.add_argument("--driver", action="store_true", help=argparse.SUPPRESS)  # set by the launchers


def check_options(args, platform):
    """Problems of options that do not apply to this run."""
    local = platform["type"] == "local"
    problems = batch_problems(args)
    problems += _conf_problems(args.conf)
    if args.detach and local:
        problems.append("--detach submits to a cluster: the job's platform is local")
    if args.log_config and not local and not args.driver:
        problems.append("--log-config is a local file: give the driver's logging with --log, --log-level")
    return problems


def _conf_problems(items):
    problems = []
    for item in items:
        key = item.split("=", 1)[0].strip()
        if "=" not in item or not key.startswith("spark."):
            problems.append("--conf %r: give spark.KEY=VALUE" % item)
        elif key in LAUNCHER_OWNED_CONF:
            problems.append("--conf %s: the launcher sets it" % key)
    return problems


def driver_flags(args):
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
    if args.interval is not None:
        flags += ["--interval", args.interval]
    flags += ["--max-messages", str(args.max_messages)]
    if args.job_key:
        flags += ["--job-key", args.job_key]
    if getattr(args, "state_lock_policy", None):
        flags += ["--state-lock-policy", args.state_lock_policy]
    if getattr(args, "state_lock_timeout", None) is not None:
        flags += ["--state-lock-timeout", str(args.state_lock_timeout)]
    if getattr(args, "failed_job_policy", None):
        flags += ["--failed-job-policy", args.failed_job_policy]
    if getattr(args, "max_job_attempts", None) is not None:
        flags += ["--max-job-attempts", str(args.max_job_attempts)]
    if args.apply_state_migrations:
        flags += ["--apply-state-migrations"]
    if getattr(args, "allow_lookup_exec", False):
        flags += ["--allow-lookup-exec"]
    if getattr(args, "fan_out_on_failure", None):
        flags += ["--fan-out-on-failure", args.fan_out_on_failure]
    return flags + _log_flags(args)


def _log_flags(args):
    """The logging and --summary options a cluster driver gets."""
    flags = []
    if getattr(args, "spark_log_level", None):
        flags += ["--spark-log-level", args.spark_log_level]
    if getattr(args, "log_level", None):
        flags += ["--log-level", args.log_level]
    for name, number in getattr(args, "log", None) or ():
        flags += ["--log", "%s=%s" % (name, logging.getLevelName(number))]
    if getattr(args, "log_format", None):
        flags += ["--log-format", args.log_format]
    if getattr(args, "log_max_chars", logs.DEFAULT_MAX_CHARS) != logs.DEFAULT_MAX_CHARS:
        flags += ["--log-max-chars", str(args.log_max_chars)]
    if getattr(args, "summary", None) == logs.LOG_SUMMARY:
        flags += ["--summary"]
    elif getattr(args, "summary", None):
        flags += ["--summary", args.summary]
    return flags
