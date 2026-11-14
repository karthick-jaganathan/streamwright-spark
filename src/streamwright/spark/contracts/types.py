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

"""Spark-specific connection and lazily imported Spark types."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from streamwright.core.jobs.types import CONTRACT_VERSION, BatchContext, Lease, Message, Observation, WriteReceipt

__all__ = [
    "CONTRACT_VERSION",
    "BatchContext",
    "Connection",
    "DataFrame",
    "Lease",
    "Message",
    "Observation",
    "SparkSession",
    "WriteReceipt",
]

DataFrame = Any
SparkSession = Any


@dataclass(frozen=True)
class Connection:
    name: str
    provider: str
    auth: Mapping[str, Any]
