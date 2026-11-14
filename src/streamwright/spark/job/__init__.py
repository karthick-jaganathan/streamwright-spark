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

"""Reading and checking SparkJob manifests."""

from streamwright.core.jobs.manifest import JobError, merge_overlay
from streamwright.core.jobs.refs import references, resolve
from streamwright.spark.job.check import NAME, RESERVED_NAMES, check_job
from streamwright.spark.job.reader import load_job
from streamwright.spark.job.validate import validate, validate_text

__all__ = [
    "NAME",
    "RESERVED_NAMES",
    "JobError",
    "check_job",
    "load_job",
    "merge_overlay",
    "references",
    "resolve",
    "validate",
    "validate_text",
]
