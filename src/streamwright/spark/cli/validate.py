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

"""sw spark validate: check jobs without Spark, brokers or credentials."""

import contextlib
import dataclasses

from streamwright.core.cli.jobs import JOB_HELP
from streamwright.core.jobs.grammar import to_internal
from streamwright.core.jobs.inputs import InputError, parse_set, read_config_values
from streamwright.spark.cli.common import err
from streamwright.spark.job import validate as validate_job
from streamwright.spark.plan import build_plan
from streamwright.spark.runtime.registry import Registry, RegistryError

__all__ = ["add_arguments", "add_plan_arguments", "run"]


def add_plan_arguments(parser):
    """`plan`: validate with the plan printed (--analyze and --graph are always on)."""
    parser.add_argument("--strict", action="store_true", help=JOB_HELP["strict"])
    parser.add_argument("--lock", metavar="FILE",
                        help="the dependency lock file (sw spark lock) to check the installed versions against")


def add_arguments(parser):
    parser.add_argument("--analyze", action="store_true",
                        help="also print the Spark actions and recomputation warnings of a valid job")
    parser.add_argument("--graph", action="store_true", help="also print each step's and output's dependencies")
    add_plan_arguments(parser)


def run(args):
    code = 0
    for path in args.job:
        try:
            config = read_config_values(args.config) if args.config else {}
            config.update(parse_set(args.set))
        except InputError as exc:
            err("%s: %s" % (path, exc))
            code = 2
            continue
        report = validate_job(
            path, overlays=args.overlay, allow=args.allow_connector, lock=args.lock, strict=args.strict, config=config
        )
        job = report.job or {}
        registry = Registry()
        adapters = {}
        for connection in (job.get("connections") or {}).values():
            if not isinstance(connection, dict):
                continue
            provider = connection.get("provider")
            if provider and provider not in adapters:
                with contextlib.suppress(RegistryError):
                    adapters[provider] = registry.load(provider, allow=args.allow_connector)
        plan = build_plan(to_internal(job), adapters) if report.valid and (args.analyze or args.graph) else None
        if plan is not None:            # the engines' internal shape (core jobs.grammar): what a run executes
            if args.analyze:
                print(plan.describe())
            if args.graph:
                for node in plan.nodes:
                    print("%-8s %-22s <- %s" % (node.kind, node.name, ", ".join(node.uses) or "-"))
        for issue in report.issues:
            err(str(issue if issue.file else dataclasses.replace(issue, file=path)))
        if plan is not None and args.analyze:
            for warning in plan.warnings:
                err("%s: warning: %s" % (path, warning))
        if not report.valid:
            code = 2
        else:
            print("%s: ok" % path)
    return code
