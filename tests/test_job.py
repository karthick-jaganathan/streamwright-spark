import copy
import os
import unittest

import sys

sys.path.insert(0, os.path.dirname(__file__))
import support  # noqa: E402

support.install_entry_points()
support.tolerate_missing_duckdb()

from streamwright.spark.job import check_job, load_job, merge_overlay, resolve  # noqa: E402
from streamwright.spark.runtime import registry  # noqa: E402

EXAMPLE = os.path.join(os.path.dirname(__file__), "fixtures", "job_fixture.yaml")
ADAPTERS = registry.load_all("adapter")[0]
LAUNCHERS = registry.load_all("launcher")[0]


def problems_of(job):
    return check_job(job, ADAPTERS, LAUNCHERS)[0]


class Example(unittest.TestCase):
    def setUp(self):
        self.job = load_job(EXAMPLE)

    def test_example_is_valid(self):
        problems, warnings = check_job(self.job, ADAPTERS, LAUNCHERS)
        self.assertEqual(problems, [])

    def test_load_from_directory(self):
        dir_path = os.path.join(os.path.dirname(__file__), "..", "examples", "s3_events_to_delta")
        job = load_job(dir_path)
        self.assertEqual(job["name"], "s3_events_to_delta")

    def test_secret_literal_is_rejected(self):
        job = copy.deepcopy(self.job)
        job["connections"]["ops_db"]["password"] = "hunter2"
        self.assertTrue(any("must be a {{ secrets.NAME }} reference" in p for p in problems_of(job)))

    def test_undeclared_secret(self):
        job = copy.deepcopy(self.job)
        job["connections"]["ops_db"]["password"] = "{{ secrets.nope }}"
        self.assertTrue(any("not declared" in p for p in problems_of(job)))

    def test_forward_reference(self):
        job = copy.deepcopy(self.job)
        job["steps"][0], job["steps"][4] = job["steps"][4], job["steps"][0]
        self.assertTrue(any("not defined before" in p for p in problems_of(job)))

    def test_write_in_steps_and_read_in_outputs(self):
        job = copy.deepcopy(self.job)
        job["steps"].append(dict(job["outputs"][0], name="bad"))
        self.assertTrue(any("writes; use it under outputs" in p for p in problems_of(job)))

    def test_bookkeeping_connection_not_used_for_outputs(self):
        job = copy.deepcopy(self.job)
        job["outputs"][4]["connection"] = "ops_db"
        self.assertTrue(any("bookkeeping" in p for p in problems_of(job)))

    def test_launcher_owned_conf(self):
        job = copy.deepcopy(self.job)
        job["runtime"]["spark"]["conf"]["spark.jars.packages"] = "x:y:1"
        self.assertTrue(any("owned by the launcher" in p for p in problems_of(job)))

    def test_unknown_named_parameter(self):
        job = copy.deepcopy(self.job)
        job["outputs"][1]["sql"] = "SELECT :tenant AS t"
        self.assertTrue(any(":tenant" in p for p in problems_of(job)))
        job["outputs"][1]["sql"] = "SELECT '12:30' AS t, 1::int AS n, :batch_id AS b"
        self.assertFalse(any("is not batch_id" in p for p in problems_of(job)))

    def test_overwrite_needs_predicate(self):
        job = copy.deepcopy(self.job)
        job["outputs"][1]["method"] = "overwrite"
        self.assertTrue(any("replace_where" in p for p in problems_of(job)))
        job["outputs"][1]["arguments"]["replace_where"] = "client_id = 'a'"
        self.assertFalse(any("replace_where" in p for p in problems_of(job)))

    def test_merge_without_predicate_warns(self):
        job = copy.deepcopy(self.job)
        del job["outputs"][0]["arguments"]["target_predicate"]
        problems, warnings = check_job(job, ADAPTERS, LAUNCHERS)
        self.assertEqual(problems, [])
        self.assertTrue(any("target_predicate" in w for w in warnings))

    def test_split_template_errors_are_reported(self):
        job = copy.deepcopy(self.job)
        job["outputs"][2]["arguments"]["file_name"] = "{client_id}_{today:%Q}.csv"
        self.assertTrue(any("unsupported directive" in p for p in problems_of(job)))
        job["outputs"][2]["arguments"]["file_name"] = "{client_id}.csv"
        job["outputs"][2]["arguments"]["layout"] = "{client_id}"
        self.assertTrue(any("event_date" in p and "collide" in p for p in problems_of(job)))

    def test_append_needs_batch_in_name(self):
        job = copy.deepcopy(self.job)
        job["outputs"][2]["arguments"]["mode"] = "append"
        self.assertTrue(any("not replay-safe" in p for p in problems_of(job)))

    def test_unknown_keys_and_arguments(self):
        job = copy.deepcopy(self.job)
        job["outputs"][1]["arguments"]["bogus"] = 1
        job["outputs"][1]["colour"] = "red"
        found = problems_of(job)
        self.assertTrue(any("unknown 'bogus'" in p for p in found))
        self.assertTrue(any("unknown key 'colour'" in p for p in found))

    def test_persist_supports_standard_pyspark_storage_levels(self):
        job = copy.deepcopy(self.job)
        for level in ("MEMORY_AND_DISK_SER", "memory_only", "OFF_HEAP", "MEMORY_ONLY_2", "disk_only_2"):
            job["steps"][-1]["persist"] = level
            self.assertEqual(problems_of(job), [])

    def test_persist_rejects_unknown_level(self):
        job = copy.deepcopy(self.job)
        job["steps"][-1]["persist"] = "invalid_cache_mode"
        problems = problems_of(job)
        self.assertTrue(any("is not a valid Spark StorageLevel" in p for p in problems), problems)


class Resolve(unittest.TestCase):
    def test_substitution_only_for_inputs(self):
        job = {"a": "{{ config.x }}/{{ steps_table.col }}", "b": ["{{ secrets.s }}"]}
        out, problems = resolve(job, {"x": "v"}, {"s": "pw"})
        self.assertEqual(out["a"], "v/{{ steps_table.col }}")
        self.assertEqual(out["b"], ["pw"])
        self.assertEqual(problems, [])

    def test_missing_value_never_prints_secrets(self):
        _, problems = resolve({"a": "{{ secrets.s }}"}, {}, {"other": "topsecret"})
        self.assertEqual(problems, ["secrets.s has no value"])

    def test_overlay_null_deletes(self):
        merged = merge_overlay({"a": {"b": 1, "c": 2}, "d": 3}, {"a": {"b": None, "e": 5}})
        self.assertEqual(merged, {"a": {"c": 2, "e": 5}, "d": 3})

    def test_load_from_s3(self):
        from unittest.mock import MagicMock, patch
        mock_body = MagicMock()
        mock_body.read.return_value = b"name: from_s3\nkind: SparkJob\nspec: {}\n"
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {"Body": mock_body}

        with patch("boto3.client", return_value=mock_s3):
            job = load_job("s3://test-bucket/jobs/s3_events_to_delta/job.yaml")
            self.assertEqual(job["name"], "from_s3")
            mock_s3.get_object.assert_called_with(Bucket="test-bucket", Key="jobs/s3_events_to_delta/job.yaml")

            # test directory resolution to job.yaml
            job_dir = load_job("s3://test-bucket/jobs/s3_events_to_delta/")
            self.assertEqual(job_dir["name"], "from_s3")
            mock_s3.get_object.assert_called_with(Bucket="test-bucket", Key="jobs/s3_events_to_delta/job.yaml")


if __name__ == "__main__":
    unittest.main()

