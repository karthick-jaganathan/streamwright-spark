"""Spark CLI submission flags and credential-safe previews."""

from types import SimpleNamespace

import pytest

from streamwright.spark.cli import main
from streamwright.spark.cli.run import _driver_flags, _preview_dry_run, _spark_conf, _submit
from streamwright.spark.contracts import LaunchError, LaunchFailureError
from streamwright.spark.launchers.local import LocalLauncher


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
        "run_key": None,
        "no_init_state": False,
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


def test_submit_forwards_shared_flags_and_does_not_ship_secret_file(capsys):
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
    assert "not delivered to cluster drivers" in capsys.readouterr().err


def test_remote_step_failure_has_execution_exit_code(capsys):
    class FailedLauncher:
        def check_lock(self, lock, platform):
            return []

        def submit(self, *args, **kwargs):
            raise LaunchFailureError("step failed: driver stopped")

    assert _submit({}, arguments(), {}, {"type": "standalone"}, FailedLauncher()) == 1
    assert "driver stopped" in capsys.readouterr().err


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
