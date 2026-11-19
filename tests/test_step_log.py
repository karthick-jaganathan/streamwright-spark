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

"""A SparkJob step's `log:` (DuckDBJob's meaning): its first rows logged on the driver, the rest counted."""

import logging
from types import SimpleNamespace

import pytest

from streamwright.core.jobs.step_log import StepLogError
from streamwright.core.runtime.redact import Redactor
from streamwright.spark.engine.executor import SparkExecutor

ROWS_SQL = "SELECT * FROM VALUES (1, 'a'), (2, NULL), (3, 'tok-SECRET') AS t(id, name)"


@pytest.fixture(scope="module")
def spark():
    sql = pytest.importorskip("pyspark.sql")
    session = (sql.SparkSession.builder.master("local[1]").appName("test_step_log")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


@pytest.fixture
def run_step(spark, caplog, monkeypatch):
    """Runs one step with `log:` in a SparkExecutor; returns its streamwright.steps lines and the counts it asked."""
    frame_type, counted = type(spark.sql("SELECT 1")), []
    count = frame_type.count
    monkeypatch.setattr(frame_type, "count", lambda frame: counted.append(1) or count(frame))

    def run(log):
        counted.clear()
        executor = SparkExecutor(spark, {"name": "j", "steps": [{"name": "s", "sql": ROWS_SQL, "log": log}],
                                         "outputs": []}, {}, {}, config={}, limits={},
                                 redact=Redactor(["tok-SECRET"]))
        executor.params = {}
        with caplog.at_level(logging.INFO, logger="streamwright.steps"):
            executor._run_steps(SimpleNamespace(builtins=lambda: {"batch.id": "b-1"}), [])
        lines = [(r.levelname, r.getMessage()) for r in caplog.records if r.name == "streamwright.steps"]
        return lines, len(counted)
    return run


def test_a_steps_rows_are_logged_and_counted_only_when_there_are_more(run_step):
    lines, counts = run_step({"message": "row {id} is {name} in {{ batch.id }}", "level": "warning", "max_rows": 2})
    assert lines == [("WARNING", "step s: row 1 is a in b-1"), ("WARNING", "step s: row 2 is null in b-1"),
                     ("WARNING", "step s: ... and 1 more row(s)")]
    assert counts == 1                              # count() ran: limit(max_rows + 1) returned more than max_rows


def test_every_row_is_logged_redacted_without_a_count_when_they_fit(run_step):
    lines, counts = run_step({"message": "{name}"})
    assert lines == [("INFO", "step s: a"), ("INFO", "step s: null"), ("INFO", "step s: ***")] and counts == 0


def test_a_placeholder_the_step_lacks_fails_it(run_step):
    with pytest.raises(StepLogError, match=r"log\.message: no column 'nope' in step s; columns: id, name"):
        run_step({"message": "{nope}"})
