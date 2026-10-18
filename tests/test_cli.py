import contextlib
import io
import os
import tempfile
import unittest

import sys

sys.path.insert(0, os.path.dirname(__file__))
import support  # noqa: E402

support.install_entry_points()
support.tolerate_missing_duckdb()

from streamwright.spark.cli import main  # noqa: E402

EXAMPLE = os.path.join(os.path.dirname(__file__), "..", "examples", "s3_events_to_delta", "job.yaml")


def run(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main(list(argv))
    return code, out.getvalue(), err.getvalue()


class Cli(unittest.TestCase):
    def test_validate_example(self):
        code, out, err = run("validate", EXAMPLE, "--analyze", "--graph", "--strict")
        self.assertEqual(code, 0, err)
        self.assertIn("ok", out)
        self.assertIn("Spark actions per batch", out)
        self.assertIn("one action (the write)", out)
        self.assertIn("event read", out)

    def test_docker_overlay_is_valid(self):
        overlay = os.path.join(os.path.dirname(__file__), "..", "overlays", "docker.yaml")
        code, out, err = run("validate", EXAMPLE, "--overlay", overlay, "--strict")
        self.assertEqual(code, 0, err)

    def test_validate_reports_problems_with_exit_2(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "bad.yaml")
            with open(path, "w") as f:
                f.write("kind: Source\nname: X\n")
            code, _, err = run("validate", path)
        self.assertEqual(code, 2)
        self.assertIn("kind must be SparkJob", err)

    def test_validate_missing_file(self):
        code, _, err = run("validate", "/nonexistent/job.yaml")
        self.assertEqual(code, 2)
        self.assertIn("cannot read", err)

    def test_dry_run_prints_plan_without_spark(self):
        code, out, err = run("run", EXAMPLE, "--dry-run", "--set", "kafka_bootstrap=k:9092", "--set", "lake_bucket=b")
        self.assertEqual(code, 0, err)
        self.assertIn("write", out)

    def test_dry_run_accepts_log_level_and_conf(self):
        code, out, err = run("run", EXAMPLE, "--dry-run", "--set", "kafka_bootstrap=k:9092", "--set", "lake_bucket=b",
                             "--log-level", "error", "--conf", "spark.sql.shuffle.partitions=4")
        self.assertEqual(code, 0, err)
        self.assertIn("write", out)

    def test_dry_run_missing_config_is_a_usage_error(self):
        code, _, err = run("run", EXAMPLE, "--dry-run")
        self.assertEqual(code, 2)
        self.assertIn("missing required config", err)

    def test_run_with_named_job_flags(self):
        # 1. --job flag
        code, out, err = run("run", "--job", EXAMPLE, "--dry-run", "--set", "kafka_bootstrap=k:9092", "--set", "lake_bucket=b")
        self.assertEqual(code, 0, err)
        self.assertIn("write", out)

        # 2. -j flag
        code, out, err = run("run", "-j", EXAMPLE, "--dry-run", "--set", "kafka_bootstrap=k:9092", "--set", "lake_bucket=b")
        self.assertEqual(code, 0, err)
        self.assertIn("write", out)

        # 3. --file flag
        code, out, err = run("run", "--file", EXAMPLE, "--dry-run", "--set", "kafka_bootstrap=k:9092", "--set", "lake_bucket=b")
        self.assertEqual(code, 0, err)
        self.assertIn("write", out)

    def test_missing_job_gives_clear_error(self):
        code, _, err = run("run")
        self.assertEqual(code, 2)
        self.assertIn("job is required", err)

    def test_no_command(self):
        code, _, _ = run()
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
