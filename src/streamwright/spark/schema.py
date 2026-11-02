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
