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
