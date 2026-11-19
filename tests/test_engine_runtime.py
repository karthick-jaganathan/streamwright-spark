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

"""Spark-specific rendering accepts the shared BatchContext field names."""

from streamwright.core.jobs import BatchContext
from streamwright.spark.engine.runtime import StepRuntime
from streamwright.spark.job.vocabulary import BUILTIN_REFERENCES


def test_runtime_uses_pinned_values_and_spark_literal_escaping():
    context = BatchContext("batch-00000007", 7, "2026-10-17T00:00:00Z", "job", 1, "store-id",
                           {"driver_host": "driver.internal"})
    manifest = {"pinned": {"rows.name": ["a\\b'c"]}}
    runtime = StepRuntime(None, context, manifest, {}, {}, limits={})
    assert runtime.text("{{ batch.seq }}-{{ driver.host }}") == "7-driver.internal"
    assert runtime.predicate("name IN ({{ rows.name }})") == "name IN ('a\\\\b''c')"


def test_every_accepted_constant_is_rendered_in_paths_and_predicates():
    context = BatchContext("batch-07", 7, "2026-10-17T00:00:00Z", "job", 1, "store-id", {"driver_host": "d"})
    runtime = StepRuntime(None, context, {}, {}, {}, limits={})
    for name in BUILTIN_REFERENCES:
        assert "{{" not in runtime.text("x/{{ %s }}/{{%s}}" % (name, name)), name
        assert "{{" not in runtime.predicate("x = {{ %s }}" % name), name
    assert runtime.text("{{ batch.ts }}/{{ batch.id_safe }}") == "2026-10-17T00:00:00Z/batch_07"
    assert runtime.predicate("seq = {{ batch.seq }}") == "seq = 7"
