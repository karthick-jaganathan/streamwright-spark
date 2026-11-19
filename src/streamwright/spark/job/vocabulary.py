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
The SparkJob words that validation and the engine share (importable without pyspark): the `persist` storage levels,
the `{{ }}` constants the engine renders, `runtime.limits` and the view a call step's `sql` reads.
"""

from types import MappingProxyType
from typing import Final

from streamwright.core.jobs.apply.rules import DEFAULT_APPLY_ROWS
from streamwright.core.jobs.fields import Field

__all__ = ["BUILTIN_REFERENCES", "LIMITS", "LIMITS_FIELD", "RECORDS_VIEW", "STORAGE_LEVELS", "limit",
           "storage_level_name"]

# the StorageLevel names PySpark has (pyspark.storagelevel.StorageLevel), and the persist shorthands
STORAGE_LEVELS: Final = frozenset((
    "DISK_ONLY", "DISK_ONLY_2", "DISK_ONLY_3", "MEMORY_AND_DISK", "MEMORY_AND_DISK_2", "MEMORY_AND_DISK_DESER",
    "MEMORY_ONLY", "MEMORY_ONLY_2", "NONE", "OFF_HEAP",
))
_SHORTHANDS: Final = MappingProxyType({"memory": "MEMORY_ONLY", "memory_and_disk": "MEMORY_AND_DISK",
                                       "disk": "DISK_ONLY"})
# the `{{ }}` constants StepRuntime.text (paths, names) and StepRuntime.predicate (SQL fragments) render
BUILTIN_REFERENCES: Final = ("batch.id", "batch.ts", "batch.id_safe", "batch.seq", "driver.host", "driver_host")
# a call step's `sql` reads the connector's records here, one JSON text `record` each (DuckDBJob's `records`)
RECORDS_VIEW: Final = "records"
# runtime.limits: DuckDBJob's keys (same meaning and defaults), plus the Spark-only max_files_per_batch (file reads)
# and call_records (a call step's records are collected on the driver; DuckDBJob streams them to its table)
LIMITS: Final = MappingProxyType({
    "reference_values": (10000, "the most distinct values (or rows) one {{ step.column }} (or {{ step }}) may pin"),
    "read_groups": (200, "the most header groups (distinct file layouts) one file read may plan"),
    "events": (10000, "the most events one publish output may produce per batch"),
    "apply_rows": (DEFAULT_APPLY_ROWS, "the most rows one apply output may send per batch"),
    "max_files_per_batch": (2000, "the most files one file read may plan per batch (Spark)"),
    "call_records": (1_000_000, "the most records one call step may collect on the driver per batch (Spark)"),
})
LIMITS_FIELD: Final = Field("object", open=True, fields={
    name: Field("integer", minimum=1, description="%s (%d)" % (text, default))
    for name, (default, text) in LIMITS.items()
})  # open: SparkRules reports a key it does not have as an error naming the engine


def storage_level_name(level):
    """The StorageLevel name of a `persist` value (a shorthand or a name, any case), or None when there is none."""
    text = str(level).strip()
    name = _SHORTHANDS.get(text.lower(), text.upper())
    return name if name in STORAGE_LEVELS else None


def limit(limits, name):
    """The value of `runtime.limits.<name>`, or its default."""
    return int((limits or {}).get(name, LIMITS[name][0]))
