"""The Spark state CLI reads and manages the shared jobs store."""

from types import SimpleNamespace

from streamwright.core.jobs import ExecutionResult, Runner
from streamwright.core.jobs.state import open_sqlite
from streamwright.spark.cli import state as state_cli
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
    monkeypatch.setattr(state_cli, "secret_values", lambda job, args: {})
    monkeypatch.setattr(state_cli, "resolve", lambda job, config, secrets: (job, []))

    def open_store(job, connections, adapters, base_dir):
        assert base_dir == str(tmp_path)
        return store

    monkeypatch.setattr(state_cli, "open_store", open_store)
    arguments = SimpleNamespace(
        allow_connector=None, job=str(tmp_path / "job.yaml"), overlay=[], arg=None, action="init"
    )
    assert state_cli.run(arguments) == 0
    assert store.schema_version() >= 1
    assert (
        Runner("sparkjob", store, SimpleNamespace(execute=lambda ctx, manifest, done, hooks: ExecutionResult())).run()
        == 0
    )
    arguments.action = "batches"
    assert state_cli.run(arguments) == 0
    assert "seq=1" in capsys.readouterr().out
    arguments.action, arguments.arg = "purge", "0"
    assert state_cli.run(arguments) == 0


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
