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

"""The Spark state CLI reads and manages the shared jobs store."""

from types import SimpleNamespace

import pytest

from streamwright.core.jobs import ExecutionResult, Runner
from streamwright.core.jobs.state import open_sqlite
from streamwright.spark.cli import state as state_cli
from streamwright.spark.cli.main import main
from streamwright.spark.job.rules import SparkRules


def test_state_cli_uses_core_schema_and_batch_fields(monkeypatch, tmp_path, capsys):
    store = open_sqlite(str(tmp_path / "spark-state.db"))
    job = {
        "name": "sparkjob",
        "runtime": {"state": {"sqlite": str(tmp_path / "spark-state.db")}, "platform": {"type": "local"}},
    }
    monkeypatch.setattr(state_cli, "load_plugins", lambda allow: ({}, {}, []))
    monkeypatch.setattr(state_cli, "load_job", lambda path, overlays: job)
    monkeypatch.setattr(state_cli, "check_job", lambda job, adapters, launchers: ([], []))
    monkeypatch.setattr(state_cli, "config_values", lambda job, args: {})
    monkeypatch.setattr(state_cli, "provided_secrets", lambda job, args: {})
    monkeypatch.setattr(state_cli, "resolve", lambda job, config, secrets: (job, []))

    def open_store(job, connections, adapters, base_dir):
        assert base_dir == str(tmp_path)
        return store

    monkeypatch.setattr(state_cli, "open_store", open_store)
    job_path = str(tmp_path / "job.yaml")
    assert main(["state", "migrate", job_path]) == 0
    assert store.schema_version() >= 1
    assert (
        Runner("sparkjob", store, SimpleNamespace(execute=lambda ctx, manifest, done, hooks: ExecutionResult())).run()
        == 0
    )
    capsys.readouterr()
    assert main(["state", "list", job_path]) == 0
    out = capsys.readouterr().out
    assert "sparkjob-00000001" in out and "completed" in out
    assert main(["state", "show", "--job", job_path, "sparkjob-00000001"]) == 0
    assert '"job_id": "sparkjob-00000001"' in (shown := capsys.readouterr().out) and '"sequence": 1' in shown
    assert main(["state", "leases", job_path]) == 0
    assert "<job>" in capsys.readouterr().out
    assert main(["state", "purge", job_path, "--older-than", "30"]) == 0   # the age in days
    assert "purged 0" in capsys.readouterr().out
    assert main(["state", "purge", job_path, "--older-than", "0s"]) == 0
    assert "purged 1" in capsys.readouterr().out
    assert main(["state", "check", job_path]) == 0
    for dropped in (["state", "init", job_path], ["state", "show", "--file", job_path, "sparkjob-00000001"],
                    ["state", "list", job_path, "--log-level", "DEBUG"], ["state", "purge", job_path, "30"]):
        with pytest.raises(SystemExit) as usage:                           # old names and options of no use here
            main(dropped)
        assert usage.value.code == 2


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
