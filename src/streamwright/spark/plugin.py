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

"""SparkJob implementation of the shared job-engine protocol."""

from streamwright.spark.cli import main as spark_main
from streamwright.spark.docs import guide as spark_guide
from streamwright.spark.job import validate, validate_text
from streamwright.spark.runtime.registry import Registry, RegistryError
from streamwright.spark.schema import json_schema as spark_schema

__all__ = ["JOB_ENGINE"]


class SparkJobEngine:
    kind = "SparkJob"

    def main(self, argv=None):
        return spark_main(argv)

    def validate_job(self, job, *, overlays=(), config=None, strict=False):
        return validate(job, overlays=overlays, config=config, strict=strict)

    def validate_job_text(self, text, *, overlays=(), config=None, strict=False):
        return validate_text(text, overlays=overlays, config=config, strict=strict)

    def json_schema(self):
        return spark_schema()

    def guide(self, section=None):
        return spark_guide(section)

    def describe_connector(self, name):
        try:
            return Registry().load(name).describe()
        except RegistryError:
            return None


JOB_ENGINE = SparkJobEngine()
