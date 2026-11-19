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

"""Spark adapter errors and launcher contracts; job types come from core."""

from streamwright.spark.contracts.adapter import AdapterError
from streamwright.spark.contracts.launcher import LAUNCHER_OWNED_CONF, Launcher, LaunchError, LaunchFailureError
from streamwright.spark.contracts.types import (
    CONTRACT_VERSION,
    BatchContext,
    Connection,
    DataFrame,
    Lease,
    Message,
    Observation,
    SparkSession,
    WriteReceipt,
)

__all__ = [
    "CONTRACT_VERSION",
    "LAUNCHER_OWNED_CONF",
    "AdapterError",
    "BatchContext",
    "Connection",
    "DataFrame",
    "LaunchError",
    "LaunchFailureError",
    "Launcher",
    "Lease",
    "Message",
    "Observation",
    "SparkSession",
    "WriteReceipt",
]
