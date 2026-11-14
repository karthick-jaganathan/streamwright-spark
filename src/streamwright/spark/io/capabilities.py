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
What the `spark` engine supports of the read and write operations, combined once from the io families' declarations
(files, databases and tables). Message operations are declared apart (`streamwright.spark.io.messaging`).
"""

from typing import Final

from streamwright.core.sdk.contract import combine_capabilities
from streamwright.spark.io import database_rules, file_rules

__all__ = ["CAPABILITIES"]

CAPABILITIES: Final = combine_capabilities(file_rules.CAPABILITIES, database_rules.CAPABILITIES)
