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

"""Examples are valid jobs through installed connector definitions."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from streamwright.core.jobs.check import check_job
from streamwright.spark.job.reader import load_job
from streamwright.spark.job.rules import SparkRules
from streamwright.spark.launchers.local import LocalLauncher
from streamwright.spark.runtime.registry import Registry

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@pytest.mark.parametrize("manifest", ["kafka/order_files.yaml", "s3/partner_orders.yaml"])
def test_example_structure_and_dependency_plan(manifest):
    job = load_job(EXAMPLES / manifest)
    adapters = {conn["provider"]: Registry().load(conn["provider"]) for conn in job["connections"].values()}
    findings = check_job(job, SparkRules(adapters, {"local": LocalLauncher()}))
    errors = [issue for issue in findings if issue.severity == "error"]
    assert errors == [], [str(issue) for issue in errors]


@pytest.mark.parametrize(
    "manifest,values",
    [
        ("kafka/order_files.yaml", ("kafka_bootstrap=localhost:9092", "lake_bucket=lake")),
        ("s3/partner_orders.yaml", ("landing_bucket=landing", "lake_bucket=lake")),
        ("google_ads/campaigns.yaml", ("lake_bucket=lake",)),
    ],
)
def test_example_validates_through_installed_spark_cli(manifest, values):
    job = EXAMPLES / manifest
    command = [str(Path(sys.executable).with_name("sw")), "spark", "validate", str(job)]
    command.extend(arg for value in values for arg in ("--set", value))
    result = subprocess.run(
        command,
        cwd=EXAMPLES.parent,
        capture_output=True,
        text=True,
        env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert str(job) + ": ok" in result.stdout
