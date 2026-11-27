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

"""
`sw spark ...`. Entry points `streamwright-spark` / `sw-spark`; attaching it to `streamwright` core is a lazy
dispatch like `streamwright mcp` (see README). Exit codes: 0 ok, 1 failed, 2 usage/validation, 130 interrupted.
Each command's options and examples: `sw spark COMMAND --help` (the descriptions below).
"""

import argparse
import sys
from types import MappingProxyType

from streamwright.core.cli.help_format import DescriptionFormatter
from streamwright.core.jobs.state import StateError
from streamwright.spark.cli import lock, run, validate
from streamwright.spark.cli.common import InputsError, add_job_arguments, err
from streamwright.spark.cli.inputs import InputError
from streamwright.spark.job import JobError
from streamwright.spark.runtime.lock import LockError

__all__ = ["build_parser", "main"]

_DESCRIPTIONS = MappingProxyType({
    "spark": """\
Validate, plan, lock and run kind: SparkJob manifests on a Spark platform (runtime.platform: local, EMR, ...),
and manage their batches. `sw validate|plan|run JOB` routes a SparkJob here too; this is the explicit form.
Exit codes: 0 success, 1 invalid job or failed run, 2 usage or input error, 130 interrupted.""",
    "validate": """\
Check SparkJob manifests offline, without Spark, brokers or secret values: structure, inputs, connections
(against the installed connectors' contracts), steps, outputs, references, runtime and platform.

  sw spark validate jobs/orders.yaml --set client=acme --analyze
  sw spark validate jobs/*.yaml --strict --lock orders.lock.json""",
    "plan": """\
Print a valid job's Spark actions, recomputation warnings and step dependencies (validate --analyze --graph).

  sw spark plan jobs/orders.yaml""",
    "run": """\
Run a SparkJob on its platform: locally in this process, or submitted to a cluster (spark-submit, an EMR step).
The batches its runtime.state scope left unfinished resume first, then new batches run. A job that consumes a
broker (a step with method: consume) polls a batch of messages, commits its outputs and
outbox events, then acknowledges the messages; --drain, --available-now and --follow make more batches.

  sw spark run jobs/orders.yaml --secrets secrets.yaml --set client=acme
  sw spark run jobs/order_events.yaml --available-now --max-messages 5000 --summary
  sw spark run jobs/orders.yaml --lock orders.lock.json --lock-required --detach
  sw spark run jobs/orders.yaml --dry-run""",
    "lock": """\
Write the job's dependency lock file: the Python, Spark and package versions installed here, and the JARs its
connectors need, so a cluster runs the versions it was tested with. --check verifies an existing one instead.

  sw spark lock jobs/orders.yaml --spark 3.5.3 --emr-release emr-7.2.0 --out orders.lock.json
  sw spark lock jobs/orders.yaml --out orders.lock.json --check""",
})

# (name, help, the job options it takes: {many, inputs} or None when the command adds its own, its options, handler)
_COMMANDS = (
    ("validate", "check jobs without Spark, brokers or credentials", {"many": True}, validate.add_arguments,
     validate.run),
    ("plan", "print the Spark actions and step dependencies (validate --analyze --graph)",
     {"many": True}, validate.add_plan_arguments, None),
    ("run", "run a SparkJob: resume unfinished batches, then run new ones", {}, run.add_arguments, run.run),
    ("lock", "write or check the dependency lock file (package, Spark and JAR versions)", {"inputs": False},
     lock.add_arguments, lock.run),
)


def build_parser():
    """The `sw spark` argument parser (validate, plan, run, lock)."""
    # no prefix matching: an unknown --log must not be taken for --log-level (Spark's level)
    raw = DescriptionFormatter
    parser = argparse.ArgumentParser(prog="sw spark", description=_DESCRIPTIONS["spark"], formatter_class=raw,
                                     allow_abbrev=False)
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    for name, help_text, job_options, add_arguments, handler in _COMMANDS:
        command = sub.add_parser(name, help=help_text, description=_DESCRIPTIONS.get(name), formatter_class=raw,
                                 allow_abbrev=False)
        if job_options is not None:
            add_job_arguments(command, **job_options)
        add_arguments(command)
        command.set_defaults(handler=handler or _run_plan)
    return parser


def _run_plan(args):
    args.analyze = True
    args.graph = True
    return validate.run(args)


def _normalize_job_args(args):
    if hasattr(args, "flag_job"):
        flag_val = args.flag_job
        if flag_val:
            if args.job and args.job != flag_val:
                err("error: pass the job once: JOB or --job JOB")
                return False
            args.job = flag_val
        elif not args.job:
            err("error: job is required (pass JOB positional argument or --job PATH)")
            return False

    if hasattr(args, "flag_jobs"):
        jobs = list(args.job or []) + list(args.flag_jobs or [])
        if not jobs:
            err("error: at least one job path is required (pass PATH positional argument or --job PATH)")
            return False
        args.job = jobs

    return True


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else list(argv))
    if not getattr(args, "handler", None):
        parser.print_help()
        return 2
    if not _normalize_job_args(args):
        return 2
    try:
        return args.handler(args)
    except (JobError, InputError, InputsError, LockError) as exc:
        err("error: %s" % exc)
        return 2
    except StateError as exc:                             # e.g. a migration is pending
        err("error: %s" % exc)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
