"""The SparkJob `runtime` checks (state, env, Spark conf) and where it runs (platform, launcher, cluster secrets)."""

import os
import re

from streamwright.core.jobs.refs import references
from streamwright.spark.contracts import LAUNCHER_OWNED_CONF

__all__ = ["check_platform", "check_runtime"]


def check_runtime(job):
    """Problems with `runtime`: required, the bookkeeping connection, `env` names/values and `spark.conf` keys."""
    runtime = job.get("runtime")
    if not isinstance(runtime, dict):
        return [("missing-runtime", ("runtime",), "runtime is required", False)]
    found = []
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
    if platform.get("type") not in (None, "local") and any(
        isinstance(value, str) and any(ref.startswith("secrets.") for ref in references(value))
        for value in environment.values()
    ):
        secret_provider = runtime.get("secrets") if isinstance(runtime.get("secrets"), dict) else {}
        if secret_provider.get("source") != "aws_secretsmanager":
            found.append(
                (
                    "secret-provider",
                    ("runtime", "secrets"),
                    "cluster secrets require a driver-side secret provider",
                    False,
                )
            )
    launcher = launchers.get(platform.get("type")) if launchers is not None else None
    if launchers is not None:
        if launcher is None:
            found.append(("no-launcher", ("runtime", "platform"), "no launcher for this platform", False))
        else:
            found.extend(
                ("bad-platform", ("runtime", "platform"), text, False) for text in launcher.check_platform(platform)
            )
    return found
