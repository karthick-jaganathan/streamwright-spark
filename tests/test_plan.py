import copy
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import support  # noqa: E402

support.install_entry_points()

from streamwright.spark.job import load_job  # noqa: E402
from streamwright.spark.plan import build_plan as _build_plan  # noqa: E402
from streamwright.spark.runtime import registry  # noqa: E402

ADAPTERS = registry.load_all("adapter")[0]


def build_plan(job):
    return _build_plan(job, ADAPTERS)

EXAMPLE = os.path.join(os.path.dirname(__file__), "fixtures", "job_fixture.yaml")


class Plan(unittest.TestCase):
    def setUp(self):
        self.job = load_job(EXAMPLE)

    def test_example_has_no_warnings(self):
        self.assertEqual(build_plan(self.job).warnings, [])

    def test_one_write_action_per_output(self):
        plan = build_plan(self.job)
        writes = [a for a in plan.actions if a[0] == "write"]
        self.assertEqual(len(writes), len(self.job["outputs"]) - 1)          # announce publishes: an event read
        self.assertEqual(len([a for a in plan.actions if a[0] == "event read"]), 1)

    def test_mapping_tables_are_pinned(self):
        plan = build_plan(self.job)
        self.assertEqual(sorted(plan.pinned_steps), ["canonical_columns", "client_columns"])
        text = "\n".join(t for k, t in plan.actions if k == "reference read")
        self.assertIn("pinned", text)
        self.assertIn("valid_arrivals", text)                  # a column reference: collected once

    def test_persist_is_a_barrier(self):
        plan = build_plan(self.job)
        # orders is only reached through tagged_orders (persisted), so it has one evaluation root, not four outputs
        self.assertEqual(plan.node("orders").roots, {"tagged_orders"})
        self.assertEqual(plan.node("tagged_orders").roots,               # the three outputs that read it
                         {"bronze_orders", "client_exports", "ingest_audit"})
        self.assertIn("client_exports", plan.node("orders").outputs)

    def test_removing_persist_warns(self):
        job = copy.deepcopy(self.job)
        del job["steps"][-1]["persist"]
        plan = build_plan(job)
        self.assertTrue(any(w.startswith("tagged_orders is") for w in plan.warnings), plan.warnings)

    def test_unneeded_persist_warns(self):
        job = copy.deepcopy(self.job)
        job["steps"][2]["persist"] = "memory"                  # arrivals: one evaluation root once outputs are trimmed
        job["outputs"] = job["outputs"][:1]
        plan = build_plan(job)
        self.assertTrue(any("persisted but only" in w for w in plan.warnings), plan.warnings)

    def test_column_reference_to_expensive_step_warns(self):
        job = copy.deepcopy(self.job)
        job["steps"][3]["connection"] = "config_db"            # valid_arrivals now reads a database
        job["steps"][3].update(service="database", method="query", arguments={"query": "SELECT 1"})
        plan = build_plan(job)
        self.assertTrue(any(w.startswith("valid_arrivals is collected") for w in plan.warnings), plan.warnings)

    def test_header_reads_are_driver_side(self):
        plan = build_plan(self.job)
        self.assertTrue(any(k == "driver reads" and t.startswith("orders") for k, t in plan.actions))


if __name__ == "__main__":
    unittest.main()
