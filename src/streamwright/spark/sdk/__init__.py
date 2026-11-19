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

"""The Spark engine contract for declarative connectors."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from streamwright.core.jobs import WriteReceipt
from streamwright.core.jobs.types import Observation
from streamwright.core.sdk.contract import Operation, ResolvedConnection

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from streamwright.spark.engine.runtime import StepRuntime

__all__ = ["Observation", "SparkAdapter", "SparkContext", "WriteReceipt"]


@dataclass(frozen=True)
class SparkContext:
    """The batch runtime and the name of the step or output being executed."""

    runtime: "StepRuntime"
    name: str
    connection: ResolvedConnection

    @property
    def spark(self) -> "SparkSession":
        return self.runtime.spark


class SparkAdapter(Protocol):
    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> Mapping[str, str]: ...

    def read(self, context: SparkContext, operation: Operation, arguments: Mapping[str, Any]) -> "DataFrame": ...

    def write(
        self, context: SparkContext, frame: "DataFrame", operation: Operation, arguments: Mapping[str, Any]
    ) -> WriteReceipt: ...

