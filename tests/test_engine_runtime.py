"""Spark-specific rendering accepts the shared BatchContext field names."""

from streamwright.core.jobs import BatchContext
from streamwright.spark.engine.runtime import StepRuntime


def test_runtime_uses_pinned_values_and_spark_literal_escaping():
    context = BatchContext("batch-00000007", 7, "2026-10-17T00:00:00Z", "job", 1, "store-id",
                           {"driver_host": "driver.internal"})
    manifest = {"pinned": {"rows.name": ["a\\b'c"]}}
    runtime = StepRuntime(None, context, manifest, {}, {}, limits={})
    assert runtime.text("{{ batch.seq }}-{{ driver.host }}") == "7-driver.internal"
    assert runtime.predicate("name IN ({{ rows.name }})") == "name IN ('a\\\\b''c')"
