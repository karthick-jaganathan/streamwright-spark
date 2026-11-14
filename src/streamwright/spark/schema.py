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

"""SparkJob JSON schema: shared job shape plus declared connector field schemas."""

from streamwright.core.jobs.schema import json_schema as jobs_schema
from streamwright.spark.job.rules import SparkRules
from streamwright.spark.runtime.registry import Registry

__all__ = ["json_schema"]


def json_schema(entry_points=None):
    adapters, _ = Registry(entry_points=entry_points).load_all("adapter")
    schema = jobs_schema(SparkRules(adapters))
    connection = schema["properties"]["connections"]["additionalProperties"]
    for branch in connection.get("allOf", ()):
        provider = branch["if"]["properties"]["provider"]["const"]
        adapter = adapters[provider]
        declared = adapter.definition.json_schema("spark")
        branch["then"]["properties"].update(declared["properties"])
        branch["then"]["required"] = declared["required"]
    return schema
