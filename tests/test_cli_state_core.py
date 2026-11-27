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

"""A SparkJob's batches are looked after by `streamwright state ... --url`, from the shared state tables alone."""

import json
from types import SimpleNamespace

from streamwright.core.cli import main as core_main
from streamwright.core.jobs import ExecutionResult, Runner
from streamwright.core.jobs.state import open_sqlite
from streamwright.spark.cli import state as state_cli
from streamwright.spark.job.rules import SparkRules


def test_streamwright_state_reads_and_settles_a_spark_job_s_batches_without_the_job(tmp_path, capsys):
    path = tmp_path / "spark-state.db"
    store = open_sqlite(str(path))
    store.initialize()
    assert (
        Runner("sparkjob", store, SimpleNamespace(execute=lambda ctx, manifest, done, hooks: ExecutionResult())).run()
        == 0
    )
    url = "sqlite:///%s" % path
    capsys.readouterr()
    assert core_main(["state", "list", "--url", url, "--json"]) == 0
    (listed,) = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert (listed["job"], listed["job_id"], listed["status"]) == ("sparkjob", "sparkjob-00000001", "completed")
    assert core_main(["state", "show", "--url", url, "sparkjob-00000001"]) == 0
    assert json.loads(capsys.readouterr().out)["sequence"] == 1
    assert core_main(["state", "leases", "--url", url, "--job", "sparkjob"]) == 0
    assert "<job>" in capsys.readouterr().out
    assert core_main(["state", "purge", "--url", url, "--job", "sparkjob", "--older-than", "0s"]) == 0
    assert "purged 1" in capsys.readouterr().out
    assert core_main(["state", "check", "--url", url]) == 0


def test_sqlite_state_resolves_relative_to_job_directory_not_current_directory(tmp_path, monkeypatch):
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    cwd = tmp_path / "elsewhere"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    job = {"runtime": {"state": {"sqlite": "state.sqlite"}, "platform": {"type": "local"}}}
    store = state_cli.open_store(job, {}, {}, str(job_dir))
    store.initialize()
    assert (job_dir / "state.sqlite").is_file()
    assert not (cwd / "state.sqlite").exists()


def test_remote_job_rejects_relative_sqlite_state():

    job = {"runtime": {"state": {"sqlite": "state.sqlite"}, "platform": {"type": "standalone"}}}
    assert (
        "bad-state",
        ("runtime", "state", "sqlite"),
        "remote jobs require an absolute runtime.state.sqlite path",
        False,
    ) in SparkRules({}).check_engine(job)
    try:
        state_cli.open_store(job, {}, {}, "/cluster/job")
    except ValueError as exc:
        assert "absolute" in str(exc)
    else:
        raise AssertionError("relative sqlite state must not be opened on a remote driver")
