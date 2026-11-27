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

"""The SparkJob `runtime` checks (state, env, Spark conf) and where it runs (platform, launcher, cluster secrets)."""

import os
import re

from streamwright.core.jobs.refs import references
from streamwright.spark.contracts import LAUNCHER_OWNED_CONF
from streamwright.spark.job.vocabulary import LIMITS

__all__ = ["check_platform", "check_runtime"]


def check_runtime(job):
    """
    Problems with `runtime`: required, the bookkeeping connection, `limits` keys, `env` names/values and `spark.conf`
    keys.
    """
    runtime = job.get("runtime")
    if not isinstance(runtime, dict):
        return [("missing-runtime", ("runtime",), "runtime is required", False)]
    limits = runtime.get("limits") if isinstance(runtime.get("limits"), dict) else {}
    found = [("unsupported-key", ("runtime", "limits", str(key)), "the Spark engine has no runtime.limits.%s (keys: %s)"
              % (key, ", ".join(sorted(LIMITS))), False) for key in limits if key not in LIMITS]
    state = runtime.get("state") if isinstance(runtime.get("state"), dict) else {}
    if state.get("connection"):
        for index, output in enumerate(job.get("outputs") or ()):
            if isinstance(output, dict) and output.get("connection") == state["connection"]:
                found.append(
                    (
                        "state-output",
                        ("outputs", index, "connection"),
                        "outputs cannot write through the bookkeeping connection",
                        False,
                    )
                )
    environment = runtime.get("env") if isinstance(runtime.get("env"), dict) else {}
    if isinstance(runtime.get("env"), dict):
        for key, value in environment.items():
            if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                found.append(("bad-env", ("runtime", "env", str(key)), "invalid environment name", False))
            if isinstance(value, (dict, list)):
                found.append(
                    ("bad-env", ("runtime", "env", str(key)), "environment variables must be scalar values", False)
                )
    spark = runtime.get("spark") if isinstance(runtime.get("spark"), dict) else {}
    checkpoint_dir = (spark.get("conf") or {}).get("spark.checkpoint.dir")
    for index, step in enumerate(job.get("steps") or ()):
        if isinstance(step, dict) and step.get("checkpoint") and not checkpoint_dir:
            found.append((
                "checkpoint-dir",
                ("steps", index, "checkpoint"),
                "checkpoint requires runtime.spark.conf.spark.checkpoint.dir",
                False,
            ))
    for key in spark.get("conf") or {}:
        if not isinstance(key, str) or key in LAUNCHER_OWNED_CONF or not key.startswith("spark."):
            found.append(
                (
                    "bad-spark-conf",
                    ("runtime", "spark", "conf", key),
                    "Spark configuration is launcher-owned or lacks the spark. prefix",
                    False,
                )
            )
    return found


def _cluster_secrets(job, environment):
    """
    On a cluster the driver has no --secrets file: a secret that runtime.env references must be bound to a lookup
    (spec.secrets.NAME.lookup, such as an aws_secretsmanager lookup), which the driver resolves.
    """
    spec = job.get("spec") if isinstance(job.get("spec"), dict) else {}
    declared = spec.get("secrets") if isinstance(spec.get("secrets"), dict) else {}
    found = []
    for key, value in environment.items():
        for ref in references(value) if isinstance(value, str) else ():
            name = ref[len("secrets."):] if ref.startswith("secrets.") else None
            if name and not (isinstance(declared.get(name), dict) and declared[name].get("lookup")):
                found.append(("secret-provider", ("runtime", "env", key),
                              "secrets.%s: on a cluster the driver resolves secrets through lookups; bind it with "
                              "spec.secrets.%s.lookup (e.g. an aws_secretsmanager lookup)" % (name, name), False))
    return found


def check_platform(job, launchers):
    """Problems with where the job runs: remote state paths, cluster secrets and its launcher (`launchers` None: not
    checked)."""
    runtime = job.get("runtime")
    if not isinstance(runtime, dict):
        return []
    found = []
    state = runtime.get("state") if isinstance(runtime.get("state"), dict) else {}
    environment = runtime.get("env") if isinstance(runtime.get("env"), dict) else {}
    platform = runtime.get("platform") if isinstance(runtime.get("platform"), dict) else {}
    sqlite_path = state.get("sqlite")
    if (
        isinstance(sqlite_path, str)
        and platform.get("type") not in (None, "local")
        and not os.path.isabs(sqlite_path)
    ):
        found.append(
            (
                "bad-state",
                ("runtime", "state", "sqlite"),
                "remote jobs require an absolute runtime.state.sqlite path",
                False,
            )
        )
    if platform.get("type") not in (None, "local"):
        found.extend(_cluster_secrets(job, environment))
    launcher = launchers.get(platform.get("type")) if launchers is not None else None
    if launchers is not None:
        if launcher is None:
            found.append(("no-launcher", ("runtime", "platform"), "no launcher for this platform", False))
        else:
            found.extend(
                ("bad-platform", ("runtime", "platform"), text, False) for text in launcher.check_platform(platform)
            )
    return found
