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

"""Spark messaging capabilities and broker lifecycle, shared by every connector."""

from typing import Final

from streamwright.core.sdk.messaging import job_capabilities

__all__ = ["CAPABILITIES", "open_broker"]


def open_broker(adapter, connection):
    """Open a definition's broker without storing credentials in Spark."""
    return adapter.adapter.open_broker(connection)


CAPABILITIES: Final = job_capabilities("spark")
