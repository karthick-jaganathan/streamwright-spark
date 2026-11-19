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

"""Spark CLI submission flags and credential-safe previews."""

from types import SimpleNamespace

import pytest

from streamwright.core.runtime import logs
from streamwright.core.runtime.logs.lines import utc_now
from streamwright.spark.cli import main
from streamwright.spark.cli import run as run_module
from streamwright.spark.cli.run import _preview_dry_run, _spark_conf, _submit
from streamwright.spark.cli.run_options import check_options as _check_options
from streamwright.spark.cli.run_options import driver_flags as _driver_flags
from streamwright.spark.cli.run_summary import summary as _summary
from streamwright.spark.contracts import LaunchError, LaunchFailureError
from streamwright.spark.launchers.local import LocalLauncher
from streamwright.spark.runtime import lock as lockmod


def arguments(**options):
    defaults = {
        "overlay": [],
        "set": [],
        "config": None,
        "follow": False,
        "available_now": False,
        "drain": False,
        "interval": "10s",
        "max_messages": 500,
        "job_key": None,
        "apply_state_migrations": False,
        "log_level": None,
        "conf": [],
        "job": "job.yaml",
        "lock": None,
        "secrets": None,
        "detach": False,
    }
    defaults.update(options)
    return SimpleNamespace(**defaults)


def test_missing_job_and_empty_command(capsys):
    assert main([]) == 2
    assert main(["run"]) == 2
    assert "job is required" in capsys.readouterr().err


def test_options_are_not_abbreviated(capsys):
    # --spark-log is not --spark-log-level cut short
    with pytest.raises(SystemExit) as usage:
        main(["run", "job.yaml", "--spark-log", "WARN"])
    assert usage.value.code == 2 and "unrecognized arguments: --spark-log WARN" in capsys.readouterr().err


def test_shared_driver_flags_preserve_overlay_and_config():
    flags = _driver_flags(
        arguments(
            overlay=["config/production.yaml"],
            set=["tenant=a"],
            config="inputs/tenant.yaml",
            follow=True,
            max_messages=25,
        )
    )
    assert flags == [
        "--overlay",
        "production.yaml",
        "--set",
        "tenant=a",
        "--config",
        "tenant.yaml",
        "--follow",
        "--interval",
        "10s",
        "--max-messages",
        "25",
    ]


def test_environment_secrets_are_not_submitted():
    job = {
        "runtime": {"env": {"STREAMWRIGHT_SECRET_TOKEN": "private-value", "MODE": "production"}, "spark": {"conf": {}}}
    }
    conf = _spark_conf(job, [])
    assert "private-value" not in str(conf)
    assert conf["spark.executorEnv.MODE"] == "production"
    with pytest.raises(LaunchError, match="secret"):
        _spark_conf(
            {
                "runtime": {
                    "env": {"STREAMWRIGHT_SECRET_TOKEN": "private-value"},
                    "spark": {"conf": {"spark.executorEnv.TOKEN": "private-value"}},
                }
            },
            [],
        )


def test_a_secret_in_conf_is_matched_like_redaction_not_as_any_substring():
    # a 2-character secret is too short to redact (and so to match): an ordinary path containing it is fine
    env = {"STREAMWRIGHT_SECRET_PASSWORD": "sw"}
    conf = _spark_conf({"runtime": {"env": env, "spark": {"conf": {"spark.checkpoint.dir": "/tmp/.sw-live/cp"}}}}, [])
    assert conf["spark.checkpoint.dir"] == "/tmp/.sw-live/cp"
    with pytest.raises(LaunchError, match="secret"):
        _spark_conf({"runtime": {"env": {"STREAMWRIGHT_SECRET_TOKEN": "tok-12345"},
                                 "spark": {"conf": {"spark.x": "prefix-tok-12345"}}}}, [])


def test_submit_forwards_shared_flags_and_does_not_ship_secret_file(caplog):
    class FakeLauncher:
        def check_lock(self, lock, platform):
            return []

        def submit(self, job_path, argv, conf, lock, detach, **kwargs):
            assert argv == _driver_flags(args)
            assert kwargs["files"] == ["production.yaml", "tenant.yaml"]
            return 0

    args = arguments(overlay=["production.yaml"], config="tenant.yaml")
    assert _submit({}, args, {}, {"type": "standalone"}, FakeLauncher()) == 0
    args.secrets = "private.yaml"
    assert _submit({}, args, {}, {"type": "standalone"}, FakeLauncher()) == 2
    assert "not delivered to cluster drivers" in caplog.text


def test_remote_step_failure_has_execution_exit_code(caplog):
    class FailedLauncher:
        def check_lock(self, lock, platform):
            return []

        def submit(self, *args, **kwargs):
            raise LaunchFailureError("step failed: driver stopped")

    assert _submit({}, arguments(), {}, {"type": "standalone"}, FailedLauncher()) == 1
    assert [(r.name, r.levelname, r.getMessage(), r.event) for r in caplog.records] == [
        ("streamwright.spark", "ERROR", "step failed: driver stopped", "run_refused")]


def test_local_preview_uses_launcher_not_platform_switch(capsys):
    job = {
        "name": "sample",
        "steps": [],
        "outputs": [{"name": "result", "sql": "SELECT 1"}],
        "runtime": {"spark": {"conf": {}}},
    }
    _preview_dry_run(job, {}, arguments(), lock={}, platform={"type": "local"}, launcher=LocalLauncher())
    output = capsys.readouterr().out
    assert "Spark actions per batch" in output and "spark-submit" in output


def test_options_that_do_not_apply_to_the_run_are_refused():
    local, remote = {"type": "local"}, {"type": "standalone"}
    assert _check_options(arguments(interval="5s", follow=False, detach=False, log_config=None), local) == [
        "--interval is the pause of --follow"]
    assert _check_options(arguments(interval="soon", follow=True, detach=False, log_config=None), local)[0].startswith(
        "--interval: invalid duration")
    assert _check_options(arguments(interval=None, detach=True, log_config=None), local) == [
        "--detach submits to a cluster: the job's platform is local"]
    assert _check_options(arguments(interval=None, log_config="logging.yaml", driver=False), remote)
    assert _check_options(arguments(interval="1d", follow=True, detach=True, log_config=None), remote) == []
    assert _check_options(arguments(interval=None, max_messages=-1, detach=False, log_config=None), local) == [
        "--max-messages cannot be negative (0: only resume and relay)"]
    assert _check_options(arguments(interval=None, detach=False, log_config=None,
                                     conf=["spark.executor.memory=4g", "executor.cores=2", "spark.sql.x",
                                           "spark.master=local[2]"]), local) == [
        "--conf 'executor.cores=2': give spark.KEY=VALUE", "--conf 'spark.sql.x': give spark.KEY=VALUE",
        "--conf spark.master: the launcher sets it"]


def test_apply_state_migrations_is_forwarded():
    assert "--apply-state-migrations" in _driver_flags(arguments(interval=None, apply_state_migrations=True))
    assert "--apply-state-migrations" not in _driver_flags(arguments(interval=None))


def test_lock_needs_a_spark_version_to_write_and_well_formed_jars(capsys):
    assert main(["lock", "job.yaml"]) == 2
    assert "--spark VERSION is needed to write a lock" in capsys.readouterr().err
    assert main(["lock", "job.yaml", "--check", "--spark", "3.5.3", "--jar", "org.x=1"]) == 2
    assert "--jar 'org.x=1': give GROUP:ARTIFACT=VERSION" in capsys.readouterr().err


def test_driver_flags_forward_the_log_options_and_the_summary():
    flags = _driver_flags(arguments(interval=None, log_level="DEBUG", log=[("streamwright.network", 20)],
                                    log_format="json", spark_log_level="WARN", summary=logs.LOG_SUMMARY,
                                    job_key="2026-11-13", failed_job_policy="retry", max_job_attempts=5,
                                    allow_lookup_exec=True, fan_out_on_failure="stop"))
    assert "--interval" not in flags
    for pair in (["--log-level", "DEBUG"], ["--log", "streamwright.network=INFO"], ["--log-format", "json"],
                 ["--spark-log-level", "WARN"], ["--job-key", "2026-11-13"], ["--failed-job-policy", "retry"],
                 ["--max-job-attempts", "5"], ["--fan-out-on-failure", "stop"]):
        assert any(flags[i:i + 2] == pair for i in range(len(flags)))
    assert flags[-1] == "--summary" and "--allow-lookup-exec" in flags


def test_the_summary_reports_batches_outputs_and_status():
    class Store:
        def get_receipts(self, job, batch):
            return {"out": {"rows": 3, "destination": "/lake/out", "files": [], "already_committed": False}}

    runner = SimpleNamespace(processed=["j-00000001"], job_name="j", scope="client:a", skipped=False, error=None)
    started = utc_now()
    summary = _summary(arguments(), {"runner": runner, "store": Store(), "job": "j"}, started, 0, None)
    assert (summary["status"], summary["scope"], summary["job_ids"]) == ("ok", "client:a", ["j-00000001"])
    assert summary["outputs"] == [{"name": "out", "job_id": "j-00000001", "rows": 3, "files": ["/lake/out"],
                                   "already_committed": False}]
    failed = _summary(arguments(), {"redact": lambda text: text.replace("hunter2", "***")}, started, None,
                      RuntimeError("password hunter2 refused"))
    assert failed["status"] == "failed" and failed["error"] == "RuntimeError: password *** refused"
    skipped = _summary(arguments(), {"runner": SimpleNamespace(**{**vars(runner), "skipped": True, "processed": []}),
                                     "store": Store()}, started, 0, None)
    assert skipped["status"] == "skipped"


def test_a_lock_check_finds_what_the_job_needs_now():
    old = {"packages": {"streamwright-spark": "1", "streamwright-s3": "1"}, "jars": [{"coordinates": "a:b:1"}]}
    new = {"packages": {"streamwright-spark": "1", "streamwright-kafka": "2"}, "jars": [{"coordinates": "a:b:2"}]}
    assert lockmod.lock_drift(old, new) == [
        "the job now needs streamwright-kafka, which the lock does not pin",
        "the lock pins streamwright-s3, which the job no longer needs",
        "the job now needs jar a:b:2, which the lock does not pin",
        "the lock pins jar a:b:1, which the job no longer needs"]
    assert lockmod.lock_drift(old, old) == []


def test_a_run_leases_for_runtime_state_ttl_seconds(monkeypatch, tmp_path):
    captured = {}

    class Runner:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)

        def run(self, *args):
            return 0

    session = SimpleNamespace(build_session=lambda *args, **kwargs: None,
                              resolve_spark_metadata=lambda spark: ("app", "driver"))
    executor = SimpleNamespace(SparkExecutor=None)
    monkeypatch.setattr(run_module, "import_module", lambda name: session if name.endswith(".session") else executor)
    monkeypatch.setattr(run_module, "Runner", Runner)
    monkeypatch.setattr(run_module, "connections_of", lambda *args, **kwargs: {})
    monkeypatch.setattr(run_module, "open_store", lambda *args: SimpleNamespace(prepare=lambda migrate: None))
    monkeypatch.setattr(run_module, "_configure_driver_conf", lambda *args: None)
    monkeypatch.setattr(run_module, "_broker_calls", lambda *args: (None, []))
    job = {"kind": "SparkJob", "name": "j", "connections": {}, "steps": [], "outputs": [],
           "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "ttl_seconds": 45},
                       "platform": {"type": "local"}}}
    args = arguments(job=str(tmp_path / "job.yaml"), interval=None)
    assert run_module._drive_one(job, {}, args, {}, {}, lock=None, outcome={}) == 0
    assert captured["lease_seconds"] == 45


def test_validate_analyze_plans_the_jobs_internal_shape(capsys):
    assert main(["validate", "--analyze", "examples/s3/partner_orders.yaml"]) == 0
    actions = [line for line in capsys.readouterr().out.splitlines() if line.startswith("  ")]
    assert len(actions) == 6 and any("driver reads" in line for line in actions)   # operation: read as flat keys
