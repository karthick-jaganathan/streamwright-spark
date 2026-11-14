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

"""SparkExecutor through the fenced core Runner; no live Spark process required."""

import pytest

from streamwright.core.jobs import Runner, WriteReceipt
from streamwright.core.jobs.state import open_sqlite
from streamwright.core.sdk.contract import Operation, OperationKind
from streamwright.spark.contracts import Connection
from streamwright.spark.engine.executor import SparkExecutor


class Crash(BaseException):
    """A process exit does not let the runner record a failed batch."""


class FakeFrame:
    def createOrReplaceTempView(self, name):
        pass


class FakeSpark:
    def createDataFrame(self, rows, schema):
        return FakeFrame()

    def sql(self, text, args=None):
        return FakeFrame()


class FakeAdapter:
    def __init__(self):
        self.written = []
        self.crash_on = None

    def write(self, context, frame, operation, arguments):
        output = context.name
        if self.crash_on == output:
            self.crash_on = None
            raise Crash()
        self.written.append(output)
        return WriteReceipt(output, output + "/destination", 1)


def test_spark_executor_replays_only_unreceipted_outputs(tmp_path, definition_adapter):
    store = open_sqlite(str(tmp_path / "spark-state.db"))
    store.initialize()
    adapter = FakeAdapter()
    job = {
        "name": "sparkjob",
        "steps": [],
        "outputs": [
            {"name": name, "connection": "sink", "service": "data", "method": "put", "sql": "SELECT 1"}
            for name in ("first", "second")
        ],
    }
    defined = definition_adapter(
        "fake", adapter, operations=(Operation(service="data", method="put", kind=OperationKind.WRITE),)
    )
    executor = SparkExecutor(
        FakeSpark(), job, {"sink": Connection("sink", "fake", {})}, {"fake": defined}, config={}, limits={}, state=store
    )
    adapter.crash_on = "second"
    runner = Runner("sparkjob", store, executor, engine_metadata={"driver_host": "driver"})
    with pytest.raises(Crash):
        runner.run()
    assert adapter.written == ["first"]
    batch_id = runner.processed[0]
    assert store.get_batch("sparkjob", batch_id)["status"] == "processing"
    assert sorted(store.get_receipts("sparkjob", batch_id)) == ["first"]
    runner = Runner("sparkjob", store, executor, engine_metadata={"driver_host": "driver"})
    assert runner.run(max_messages=0) == 0
    assert adapter.written == ["first", "second"]
    assert store.get_batch("sparkjob", batch_id)["status"] == "completed"
