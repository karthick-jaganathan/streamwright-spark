"""
What the `spark` engine supports of the read and write operations, combined once from the io families' declarations
(files, databases and tables). Message operations are declared apart (`streamwright.spark.io.messaging`).
"""

from typing import Final

from streamwright.core.sdk.contract import combine_capabilities
from streamwright.spark.io import database_rules, file_rules

__all__ = ["CAPABILITIES"]

CAPABILITIES: Final = combine_capabilities(file_rules.CAPABILITIES, database_rules.CAPABILITIES)
