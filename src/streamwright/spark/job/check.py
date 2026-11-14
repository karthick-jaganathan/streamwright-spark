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

"""Spark checks are supplied to the shared jobs checker by SparkRules."""

import re

from streamwright.core.jobs.check import check_job as check_shared_job
from streamwright.core.jobs.check.rules import RESERVED_NAMES
from streamwright.spark.job.rules import SparkRules

__all__ = ["NAME", "RESERVED_NAMES", "check_job"]

NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def check_job(job, adapters, launchers=None):
    """Compatibility for the Spark CLI's problem/warning rendering; all checks run in core."""
    issues = check_shared_job(job, SparkRules(adapters, launchers))
    return (
        [str(issue) for issue in issues if issue.severity == "error"],
        [str(issue) for issue in issues if issue.severity == "warning"],
    )
