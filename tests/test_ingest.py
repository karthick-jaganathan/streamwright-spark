import os
import tempfile
import unittest

from streamwright.core.jobs import BatchContext
from streamwright.spark.engine.runtime import StepRuntime
from streamwright.spark.io.file_rules import check_read
from streamwright.spark.io.files import FileLocation, read_files
from streamwright.spark.sdk import SparkContext
from streamwright.spark.storage.ingest import (
    FileReject,
    Group,
    IngestError,
    SchemaReport,
    check_vocabulary,
    compute_header_hash,
    deserialize_plan,
    normalize_columns,
    normalize_row_index,
    parse_header,
    plan_ingest,
    plan_plain_groups,
    read_groups,
    resolve_file_formats,
    serialize_plan,
)


def plan_groups(files, mappings, headers, return_reports=False, **options):
    """Run the canonical core planner with the fixture's explicit mapping inputs."""
    groups, rejects, reports = plan_ingest(files, mappings, headers, **options)
    return (groups, rejects, reports) if return_reports else (groups, rejects)


try:
    from pyspark.sql import SparkSession

    _HAVE_SPARK = True
except ImportError:
    _HAVE_SPARK = False


def mapping_rows():
    return [
        {"prefix": "s3a://l/a/", "header": "timestamp", "canonical": "event_ts", "type": "timestamp", "required": True},
        {
            "prefix": "s3a://l/a/",
            "header": "AMO ID (evar8)",
            "canonical": "amo_id",
            "type": "string",
            "required": False,
        },
        {"prefix": "s3a://l/a/", "header": "revenue", "canonical": "revenue", "type": "double", "required": False},
        {"prefix": "s3a://l/b/", "header": "timestamp", "canonical": "event_ts", "type": "timestamp", "required": True},
        {
            "prefix": "s3a://l/b/",
            "header": "AMO_ID (evar18)",
            "canonical": "amo_id",
            "type": "string",
            "required": False,
        },
        {"prefix": "s3a://l/b/", "header": "Revenue", "canonical": "revenue", "type": "double", "required": False},
        {"prefix": "s3a://l/b/vip/", "header": "ts", "canonical": "event_ts", "type": "timestamp", "required": True},
    ]


def vocabulary_rows():
    return [
        {"name": "event_ts", "type": "timestamp"},
        {"name": "amo_id", "type": "string"},
        {"name": "revenue", "type": "double"},
    ]


class Groups(unittest.TestCase):
    def test_two_clients_two_headers(self):
        files = ["s3a://l/a/1.csv", "s3a://l/a/2.csv", "s3a://l/b/1.csv"]
        headers = {
            "s3a://l/a/1.csv": ["timestamp", "AMO ID (evar8)", "revenue", "junk"],
            "s3a://l/a/2.csv": ["timestamp", "AMO ID (evar8)", "revenue", "junk"],
            "s3a://l/b/1.csv": ["timestamp", "AMO_ID (evar18)", "Revenue"],
        }
        groups, rejects = plan_groups(files, mapping_rows(), headers)
        self.assertEqual(rejects, [])
        self.assertEqual([len(g.files) for g in groups], [2, 1])
        self.assertEqual([c[1] for c in groups[0].columns], ["event_ts", "amo_id", "revenue"])
        self.assertEqual([c[0] for c in groups[1].columns], ["timestamp", "AMO_ID (evar18)", "Revenue"])

    def test_longest_prefix_wins(self):
        groups, rejects = plan_groups(["s3a://l/b/vip/1.csv"], mapping_rows(), {"s3a://l/b/vip/1.csv": ["ts", "x"]})
        self.assertEqual(rejects, [])
        self.assertEqual(groups[0].prefix, "s3a://l/b/vip/")

    def test_rejects_do_not_fail_other_files(self):
        files = ["s3a://l/a/1.csv", "s3a://l/a/2.csv", "s3a://l/zzz/1.csv", "s3a://l/a/3.csv"]
        headers = {
            "s3a://l/a/1.csv": ["timestamp", "revenue"],
            "s3a://l/a/2.csv": ["revenue"],
            "s3a://l/zzz/1.csv": ["x"],
            "s3a://l/a/3.csv": [],
        }
        groups, rejects = plan_groups(files, mapping_rows(), headers)
        self.assertEqual(len(groups), 1)
        reasons = {r.path: r.reason for r in rejects}
        self.assertIn("missing required", reasons["s3a://l/a/2.csv"])
        self.assertEqual(reasons["s3a://l/zzz/1.csv"], "no column mapping for this path")
        self.assertIn("header", reasons["s3a://l/a/3.csv"])

    def test_underscore_in_prefix_is_literal(self):
        rows = [
            {"prefix": "s3a://l/a_b/", "header": "t", "canonical": "event_ts", "type": "timestamp", "required": False}
        ]
        _, rejects = plan_groups(["s3a://l/aXb/1.csv"], rows, {"s3a://l/aXb/1.csv": ["t"]})
        self.assertEqual(rejects[0].reason, "no column mapping for this path")

    def test_two_columns_to_one_name(self):
        rows = [
            *mapping_rows()[:3],
            {"prefix": "s3a://l/a/", "header": "rev2", "canonical": "revenue", "type": "double", "required": False},
        ]
        _, rejects = plan_groups(["s3a://l/a/1.csv"], rows, {"s3a://l/a/1.csv": ["timestamp", "revenue", "rev2"]})
        self.assertIn("both map", rejects[0].reason)

    def test_path_regex_overrides_prefix(self):
        # A general prefix rule exists, but a path_regex rule targets specific filenames
        rows = [
            {"prefix": "s3a://l/inbox/", "header": "ts", "canonical": "event_ts", "type": "timestamp"},
            {
                "path_regex": r".*/inbox/.*_vip_.*\.csv$",
                "header": "vip_ts",
                "canonical": "event_ts",
                "type": "timestamp",
            },
        ]
        files = ["s3a://l/inbox/standard_01.csv", "s3a://l/inbox/2026_vip_orders.csv"]
        headers = {
            "s3a://l/inbox/standard_01.csv": ["ts"],
            "s3a://l/inbox/2026_vip_orders.csv": ["vip_ts"],
        }
        groups, rejects = plan_groups(files, rows, headers)
        self.assertEqual(rejects, [])
        self.assertEqual(len(groups), 2)
        # Verify the VIP file matched the path_regex rule group
        vip_group = next(g for g in groups if "2026_vip_orders.csv" in g.files[0])
        self.assertIn("vip", vip_group.prefix)
        self.assertEqual(vip_group.columns[0][0], "vip_ts")

    def test_header_regex_matching(self):
        rows = [
            {
                "prefix": "s3a://l/a/",
                "regex": r"(?i)^order[_\s]?(num|id|no)$",
                "canonical": "order_id",
                "type": "bigint",
                "required": True,
            },
            {
                "prefix": "s3a://l/a/",
                "regex": r"(?i)^cust(omer)?[_\s]?id$",
                "canonical": "customer_id",
                "type": "string",
            },
            {"prefix": "s3a://l/a/", "header": "amount", "canonical": "amount", "type": "double"},
        ]
        files = ["s3a://l/a/1.csv", "s3a://l/a/2.csv"]
        headers = {
            "s3a://l/a/1.csv": ["Order_Num", "Cust_ID", "amount"],
            "s3a://l/a/2.csv": ["order id", "customer_id", "amount"],
        }
        groups, rejects = plan_groups(files, rows, headers)
        self.assertEqual(rejects, [])
        self.assertEqual(len(groups), 2)
        for g in groups:
            canonical_names = [c[1] for c in g.columns]
            self.assertEqual(canonical_names, ["order_id", "customer_id", "amount"])

    def test_header_regex_collision_detected(self):
        rows = [
            {"prefix": "s3a://l/a/", "regex": r"(?i)^order[_\s]?(num|id)$", "canonical": "order_id", "type": "bigint"},
        ]
        files = ["s3a://l/a/1.csv"]
        # Both Order_Num and order_id match the same regex for canonical order_id
        headers = {"s3a://l/a/1.csv": ["Order_Num", "order_id"]}
        groups, rejects = plan_groups(files, rows, headers)
        self.assertEqual(groups, [])
        self.assertEqual(len(rejects), 1)
        self.assertIn("both map to order_id", rejects[0].reason)

    def test_header_regex_missing_required(self):
        rows = [
            {
                "prefix": "s3a://l/a/",
                "regex": r"(?i)^order[_\s]?(num|id)$",
                "canonical": "order_id",
                "type": "bigint",
                "required": True,
            },
        ]
        files = ["s3a://l/a/1.csv"]
        headers = {"s3a://l/a/1.csv": ["unrelated_col"]}
        groups, rejects = plan_groups(files, rows, headers)
        self.assertEqual(groups, [])
        self.assertEqual(len(rejects), 1)
        self.assertIn("missing required column(s): order_id", rejects[0].reason)

    def test_group_limit(self):
        files = ["s3a://l/a/%d.csv" % i for i in range(3)]
        headers = {f: ["timestamp"] + ["revenue"] * i for i, f in enumerate(files)}
        headers["s3a://l/a/2.csv"] = ["timestamp", "revenue", "AMO ID (evar8)"]
        with self.assertRaises(IngestError):
            plan_groups(files, mapping_rows(), headers, max_groups=1)


class PlainGroups(unittest.TestCase):
    def test_plain_groups_by_name(self):
        files = ["s3a://l/1.csv", "s3a://l/2.csv", "s3a://l/3.csv"]
        headers = {
            "s3a://l/1.csv": ["col_a", "col_b"],
            "s3a://l/2.csv": ["col_b", "col_a"],  # reordered
            "s3a://l/3.csv": ["col_a", "col_b"],  # same as 1
        }
        formats = dict.fromkeys(files, "csv")
        groups, rejects, reports = plan_plain_groups(files, headers, formats, schema_mode="by_name")
        self.assertEqual(rejects, [])
        # Two distinct header signatures -> 2 groups
        self.assertEqual(len(groups), 2)
        group1 = next(g for g in groups if g.header == ["col_a", "col_b"])
        group2 = next(g for g in groups if g.header == ["col_b", "col_a"])
        self.assertEqual(sorted(group1.files), ["s3a://l/1.csv", "s3a://l/3.csv"])
        self.assertEqual(group2.files, ["s3a://l/2.csv"])
        self.assertEqual(len(reports), 3)
        self.assertTrue(all(r.status == "ok" for r in reports))

    def test_plain_groups_strict_identical_headers(self):
        files = ["s3a://l/1.csv", "s3a://l/2.csv"]
        headers = {"s3a://l/1.csv": ["a", "b"], "s3a://l/2.csv": ["a", "b"]}
        formats = dict.fromkeys(files, "csv")
        groups, rejects, _reports = plan_plain_groups(files, headers, formats, schema_mode="strict")
        self.assertEqual(rejects, [])
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0].files), 2)

    def test_plain_groups_strict_rejects_differing_file(self):
        files = ["s3a://l/1.csv", "s3a://l/2.csv", "s3a://l/3.csv"]
        headers = {
            "s3a://l/1.csv": ["a", "b"],
            "s3a://l/2.csv": ["b", "a"],  # reordered
            "s3a://l/3.csv": ["a", "c"],  # differing column
        }
        formats = dict.fromkeys(files, "csv")
        groups, rejects, _reports = plan_plain_groups(files, headers, formats, schema_mode="strict")
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0].files, ["s3a://l/1.csv"])
        self.assertEqual(len(rejects), 2)
        reasons = {r.path: r.reason for r in rejects}
        self.assertIn("differing column at position 0: 'b' (expected 'a')", reasons["s3a://l/2.csv"])
        self.assertIn("differing column at position 1: 'c' (expected 'b')", reasons["s3a://l/3.csv"])


class FormatResolution(unittest.TestCase):
    def test_resolve_formats_auto(self):
        files = [
            "s3a://l/1.csv",
            "s3a://l/2.tsv",
            "s3a://l/3.parquet",
            "s3a://l/4.json",
            "s3a://l/5.jsonl",
            "s3a://l/6.csv.gz",
            "s3a://l/bad.unknown_ext",
        ]
        resolved, rejects = resolve_file_formats(files, explicit_format="auto")
        self.assertEqual(resolved["s3a://l/1.csv"], "csv")
        self.assertEqual(resolved["s3a://l/2.tsv"], "tsv")
        self.assertEqual(resolved["s3a://l/3.parquet"], "parquet")
        self.assertEqual(resolved["s3a://l/4.json"], "json")
        self.assertEqual(resolved["s3a://l/5.jsonl"], "jsonl")
        self.assertEqual(resolved["s3a://l/6.csv.gz"], "csv")
        self.assertEqual(len(rejects), 1)
        self.assertEqual(rejects[0].path, "s3a://l/bad.unknown_ext")
        self.assertIn("cannot determine format", rejects[0].reason)

    def test_explicit_format_overrides_extension(self):
        files = ["s3a://l/1.txt", "s3a://l/2.csv"]
        resolved, rejects = resolve_file_formats(files, explicit_format="csv")
        self.assertEqual(rejects, [])
        self.assertEqual(resolved["s3a://l/1.txt"], "csv")
        self.assertEqual(resolved["s3a://l/2.csv"], "csv")


class SchemaDrift(unittest.TestCase):
    def test_drift_on_unmapped_column_warn(self):
        files = ["s3a://l/a/1.csv"]
        headers = {"s3a://l/a/1.csv": ["timestamp", "AMO ID (evar8)", "revenue", "promo_code"]}
        groups, rejects, reports = plan_groups(
            files, mapping_rows(), headers, on_unmapped_column="warn", return_reports=True
        )
        self.assertEqual(rejects, [])
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(reports), 1)
        rep = reports[0]
        self.assertEqual(rep.status, "drift")
        self.assertEqual(rep.unmapped, ["promo_code"])
        self.assertEqual(sorted(rep.mapped), ["amo_id", "event_ts", "revenue"])

    def test_drift_on_unmapped_column_reject(self):
        files = ["s3a://l/a/1.csv"]
        headers = {"s3a://l/a/1.csv": ["timestamp", "AMO ID (evar8)", "revenue", "promo_code"]}
        groups, rejects, reports = plan_groups(
            files, mapping_rows(), headers, on_unmapped_column="reject", return_reports=True
        )
        self.assertEqual(len(groups), 0)
        self.assertEqual(len(rejects), 1)
        self.assertIn("unmapped column(s): promo_code", rejects[0].reason)
        self.assertEqual(reports[0].status, "rejected")

    def test_drift_on_signature_change(self):
        files = ["s3a://l/a/1.csv"]
        headers = {"s3a://l/a/1.csv": ["timestamp", "revenue"]}  # missing optional AMO ID
        old_hash = "different_hash_123"
        prev_sigs = {"s3a://l/a/": old_hash}

        _groups, rejects, reports = plan_groups(
            files, mapping_rows(), headers, previous_signatures=prev_sigs, return_reports=True
        )
        self.assertEqual(rejects, [])
        self.assertEqual(reports[0].status, "drift")
        self.assertEqual(reports[0].missing_optional, ["amo_id"])

    def test_ok_status_when_matching_previous_signature(self):
        files = ["s3a://l/a/1.csv"]
        headers = {"s3a://l/a/1.csv": ["timestamp", "revenue"]}
        curr_hash = compute_header_hash(headers["s3a://l/a/1.csv"])
        prev_sigs = {"s3a://l/a/": curr_hash}

        _groups, rejects, reports = plan_groups(
            files, mapping_rows(), headers, previous_signatures=prev_sigs, return_reports=True
        )
        self.assertEqual(rejects, [])
        self.assertEqual(reports[0].status, "ok")


class ReplayPinning(unittest.TestCase):
    def test_plan_serialization_roundtrip(self):
        cg_data = {"metrics": {"type": "double", "keep_nulls": False, "entries": [("metric_cpu", "cpu")]}}
        groups = [
            Group(
                "p1",
                ["a", "b"],
                [("a", "a", "string")],
                ["f1.csv"],
                format="csv",
                options={"sep": ","},
                column_groups=cg_data,
            )
        ]
        rejects = [FileReject("f2.csv", "missing required")]
        reports = [SchemaReport("step1", "f1.csv", "p1", "ok", "hash1", ["a"], [], [])]

        data = serialize_plan(groups, rejects, reports)
        g_out, r_out, rep_out = deserialize_plan(data)

        self.assertEqual(len(g_out), 1)
        self.assertEqual(g_out[0].prefix, "p1")
        self.assertEqual(g_out[0].files, ["f1.csv"])
        self.assertEqual(g_out[0].columns, [("a", "a", "string")])
        self.assertEqual(g_out[0].column_groups, cg_data)
        self.assertEqual(len(r_out), 1)
        self.assertEqual(r_out[0].path, "f2.csv")
        self.assertEqual(r_out[0].reason, "missing required")
        self.assertEqual(len(rep_out), 1)
        self.assertEqual(rep_out[0].status, "ok")

    def test_replay_uses_pinned_manifest_without_filesystem_reads(self):
        # Fake runtime simulating driver filesystem interaction
        class FakeRuntime:
            def __init__(self, files_fs, headers_fs):
                self.files_fs = set(files_fs)
                self.headers_fs = dict(headers_fs)
                self.first_line_calls = 0
                self.pinned = {}
                self.file_errors = []
                self.schema_reports = []
                self.max_groups = 200

            def exists(self, path):
                return path in self.files_fs

            def first_line(self, path):
                self.first_line_calls += 1
                return self.headers_fs.get(path)

            def add_file_error(self, step, path, reason):
                self.file_errors.append((step, path, reason))

            def add_schema_report(self, rep):
                self.schema_reports.append(rep)

            def path_values(self, val):
                return list(val)

            def expand(self, val):
                return val

        files = ["s3a://l/1.csv", "s3a://l/2.csv"]
        headers = {"s3a://l/1.csv": "a,b", "s3a://l/2.csv": "b,a"}
        rt = FakeRuntime(files, headers)

        # 1. Fresh batch: plan from fake filesystem
        resolved, _ = resolve_file_formats(files, "auto")
        file_headers = {p: parse_header(rt.first_line(p)) for p in files}
        self.assertEqual(rt.first_line_calls, 2)
        groups, rejects, reports = plan_plain_groups(files, file_headers, resolved, schema_mode="by_name")
        rt.pinned["ingest:read_step"] = serialize_plan(groups, rejects, reports)
        self.assertEqual(len(groups), 2)

        # 2. Mutate fake filesystem (corrupt headers, change files)
        rt.headers_fs["s3a://l/1.csv"] = "corrupted,changed"
        rt.first_line_calls = 0  # reset call counter

        # 3. Replay with pinned plan
        pinned_data = rt.pinned["ingest:read_step"]
        replayed_groups, _replayed_rejects, _replayed_reports = deserialize_plan(pinned_data)
        # Verify first_line was NEVER called on replay
        self.assertEqual(rt.first_line_calls, 0)
        # Groups match exactly original planning decisions
        self.assertEqual(len(replayed_groups), len(groups))
        self.assertEqual([g.header for g in replayed_groups], [g.header for g in groups])
        self.assertEqual([g.files for g in replayed_groups], [g.files for g in groups])


class ColumnGroups(unittest.TestCase):
    def test_plain_groups_matching_and_prefix_stripping(self):
        files = ["s3a://l/1.csv", "s3a://l/2.csv"]
        headers = {
            "s3a://l/1.csv": ["id", "metric_cpu", "metric_mem"],
            "s3a://l/2.csv": ["id", "metric_disk", "metric_cpu"],
        }
        cg_config = [{"name": "metrics", "glob": "metric_*", "type": "double", "strip_prefix": "metric_"}]
        groups, rejects, _reports = plan_plain_groups(
            files, headers, formats={"s3a://l/1.csv": "csv", "s3a://l/2.csv": "csv"}, column_groups=cg_config
        )
        self.assertEqual(rejects, [])
        self.assertEqual(len(groups), 2)
        self.assertEqual([c[1] for c in groups[0].columns], ["id"])
        self.assertEqual([c[1] for c in groups[1].columns], ["id"])
        self.assertIn("metrics", groups[0].column_groups)
        self.assertEqual(groups[0].column_groups["metrics"]["entries"], [("metric_cpu", "cpu"), ("metric_mem", "mem")])
        self.assertEqual(
            groups[1].column_groups["metrics"]["entries"], [("metric_disk", "disk"), ("metric_cpu", "cpu")]
        )

    def test_mapped_groups_with_column_groups_regex(self):
        files = ["s3a://l/a/1.csv"]
        headers = {"s3a://l/a/1.csv": ["timestamp", "revenue", "metric_latency_ms", "metric_errors"]}
        cg_config = [{"name": "metrics", "regex": r"^metric_\w+$", "type": "bigint", "strip_prefix": "metric_"}]
        groups, rejects, reports = plan_groups(
            files, mapping_rows(), headers, on_unmapped_column="reject", column_groups=cg_config, return_reports=True
        )
        self.assertEqual(rejects, [])
        self.assertEqual(len(groups), 1)
        self.assertEqual(reports[0].status, "ok")
        self.assertEqual(reports[0].unmapped, [])
        self.assertIn("metrics", reports[0].mapped)
        self.assertEqual(
            groups[0].column_groups["metrics"]["entries"],
            [("metric_latency_ms", "latency_ms"), ("metric_errors", "errors")],
        )

    def test_strip_prefix_is_optional(self):
        files = ["s3a://l/1.csv"]
        headers = {"s3a://l/1.csv": ["id", "tag_env", "tag_region"]}
        cg_config = [
            {"name": "tags", "glob": "tag_*", "type": "string"}  # strip_prefix omitted
        ]
        groups, _rejects, _reports = plan_plain_groups(
            files, headers, formats={"s3a://l/1.csv": "csv"}, column_groups=cg_config
        )
        self.assertEqual(
            groups[0].column_groups["tags"]["entries"], [("tag_env", "tag_env"), ("tag_region", "tag_region")]
        )

    def test_check_read_column_groups_validation(self):
        path = "s3://b/f.csv"
        for group in (
            {"name": "metrics", "glob": "metric_*", "type": "double"},
            {"name": "metrics", "regex": r"^metric_\d+$"},
        ):
            self.assertEqual(check_read({"path": path, "column_groups": [group]}), [])
        self.assertTrue(
            any(
                "plain identifier" in p
                for p in check_read({"path": path, "column_groups": [{"name": "123-bad", "glob": "*"}]})
            )
        )
        self.assertTrue(
            any("exactly one glob or regex" in p for p in check_read({"path": path, "column_groups": [{"name": "m"}]}))
        )
        self.assertTrue(
            any(
                "exactly one glob or regex" in p
                for p in check_read({"path": path, "column_groups": [{"name": "m", "glob": "*", "regex": ".*"}]})
            )
        )
        self.assertTrue(
            any(
                "invalid column_groups regex" in p
                for p in check_read({"path": path, "column_groups": [{"name": "m", "regex": "[bad"}]})
            )
        )
        self.assertTrue(
            any(
                "type is not supported" in p
                for p in check_read(
                    {"path": path, "column_groups": [{"name": "m", "glob": "*", "type": "invalid_type"}]}
                )
            )
        )


@unittest.skipUnless(_HAVE_SPARK, "pyspark is not installed")
class SparkMicrobatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test_microbatch")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_spark_plain_read_by_name_preserves_column_order(self):
        """
        Fixes G1: Plain CSV read with schema_mode: by_name groups by header signature
        and unions by name, preventing native Spark positional misalignment.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            f1 = os.path.join(tmpdir, "f1.csv")
            f2 = os.path.join(tmpdir, "f2.csv")
            with open(f1, "w") as f:
                f.write("col_a,col_b\n1,x\n")
            with open(f2, "w") as f:
                f.write("col_b,col_a\ny,2\n")

            files = [f1, f2]
            with open(f1) as handle1, open(f2) as handle2:
                headers = {
                    f1: parse_header(handle1.readline()),
                    f2: parse_header(handle2.readline()),
                }
            formats = dict.fromkeys(files, "csv")
            groups, rejects, _ = plan_plain_groups(files, headers, formats, schema_mode="by_name")
            self.assertEqual(rejects, [])
            df = read_groups(self.spark, groups, fmt="csv")

            rows = sorted([r.asDict() for r in df.select("col_a", "col_b").collect()], key=lambda r: r["col_a"])
            # Values land in the correct column names:
            # f1: col_a='1', col_b='x'
            # f2: col_a='2', col_b='y' (in native Spark, f2 had col_a='y', col_b='2')
            self.assertEqual(
                rows,
                [
                    {"col_a": "1", "col_b": "x"},
                    {"col_a": "2", "col_b": "y"},
                ],
            )

    def test_spark_read_mixed_csv_and_parquet(self):
        """
        Requirement R2: reads CSV and Parquet in a single batch, combining them by name.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            f_csv = os.path.join(tmpdir, "events.csv")
            f_pq = os.path.join(tmpdir, "events.parquet")
            with open(f_csv, "w") as f:
                f.write("id,event_name\n101,login\n")

            pq_df = self.spark.createDataFrame([(102, "logout")], ["id", "event_name"])
            pq_df.write.parquet(f_pq)

            files = [f_csv, f_pq]
            resolved, rejects = resolve_file_formats(files, explicit_format="auto")
            self.assertEqual(rejects, [])
            headers = {
                f_csv: ["id", "event_name"],
                f_pq: ["id", "event_name"],
            }
            groups, _, _ = plan_plain_groups(files, headers, resolved, schema_mode="by_name")
            df = read_groups(self.spark, groups)

            rows = sorted([r.asDict() for r in df.select("id", "event_name").collect()], key=lambda r: str(r["id"]))
            self.assertEqual(len(rows), 2)
            self.assertEqual(str(rows[0]["id"]), "101")
            self.assertEqual(rows[0]["event_name"], "login")
            self.assertEqual(str(rows[1]["id"]), "102")
            self.assertEqual(rows[1]["event_name"], "logout")

    def test_spark_row_errors(self):
        """
        Requirement R5: row_errors=True generates _row_errors ARRAY<STRING> with cast/required failures.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            f_csv = os.path.join(tmpdir, "orders.csv")
            with open(f_csv, "w") as f:
                f.write("order_id,amount,created_date\n1,10.5,2026-01-01\n2,bad_amount,2026-01-02\n,20.0,bad_date\n")

            group = Group(
                prefix="",
                header=["order_id", "amount", "created_date"],
                columns=[
                    ("order_id", "order_id", "bigint"),
                    ("amount", "amount", "double"),
                    ("created_date", "created_date", "date"),
                ],
                files=[f_csv],
                format="csv",
            )
            df = read_groups(self.spark, [group], row_errors=True, required_columns=["order_id"])
            rows = df.select("order_id", "amount", "created_date", "_row_errors").collect()
            r0, r1, r2 = [r.asDict() for r in rows]

            # Row 0: valid
            self.assertEqual(r0["_row_errors"], [])
            # Row 1: bad amount (cast failure)
            self.assertIn("amount", r1["_row_errors"])
            # Row 2: missing required order_id and bad date
            self.assertIn("order_id", r2["_row_errors"])
            self.assertIn("created_date", r2["_row_errors"])

    def test_read_files_through_the_io_path_with_a_columns_mapping(self):
        """
        Exercises the whole spark io.files read path (not just read_groups): a `columns:` mapping with a
        required column, which is where `ingest.is_true` is called to collect required columns.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "events.csv"), "w") as handle:
                handle.write("timestamp,revenue\n2026-01-01T00:00:00,10.5\n,20.0\n")
            prefix = tmpdir.rstrip("/") + "/"
            columns = [
                {"prefix": prefix, "header": "timestamp", "canonical": "event_ts", "type": "timestamp",
                 "required": True},
                {"prefix": prefix, "header": "revenue", "canonical": "revenue", "type": "double", "required": False},
            ]
            ctx = BatchContext("b-1", 1, "2026-10-05T00:00:00Z", "job", 1, "store-id", {})
            runtime = StepRuntime(self.spark, ctx, {}, {}, {}, limits={})
            context = SparkContext(runtime, "events", {})
            location = FileLocation((tmpdir,), "file", "file")
            frame = read_files(context, {"path": "events.csv", "format": "csv", "columns": columns,
                                         "options": {"header": True}, "row_errors": True}, location=location)
            rows = sorted((r.asDict() for r in frame.select("event_ts", "revenue", "_row_errors").collect()),
                          key=lambda r: r["revenue"])
            assert rows[0]["revenue"] == 10.5 and rows[0]["_row_errors"] == []
            assert "event_ts" in rows[1]["_row_errors"]       # the required column was missing in the second row

    def test_read_groups_with_column_groups(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            f1 = os.path.join(tmpdir, "f1.csv")
            f2 = os.path.join(tmpdir, "f2.csv")
            with open(f1, "w") as out:
                out.write("id,metric_cpu,metric_mem\n1,0.5,1024.0\n")
            with open(f2, "w") as out:
                out.write("id,metric_cpu,metric_disk\n2,0.8,500.0\n")

            cg1 = {
                "metrics": {
                    "type": "double",
                    "keep_nulls": False,
                    "entries": [("metric_cpu", "cpu"), ("metric_mem", "mem")],
                }
            }
            cg2 = {
                "metrics": {
                    "type": "double",
                    "keep_nulls": False,
                    "entries": [("metric_cpu", "cpu"), ("metric_disk", "disk")],
                }
            }
            g1 = Group(
                prefix="",
                header=["id", "metric_cpu", "metric_mem"],
                columns=[("id", "id", "int")],
                files=[f1],
                format="csv",
                column_groups=cg1,
            )
            g2 = Group(
                prefix="",
                header=["id", "metric_cpu", "metric_disk"],
                columns=[("id", "id", "int")],
                files=[f2],
                format="csv",
                column_groups=cg2,
            )

            df = read_groups(self.spark, [g1, g2])
            rows = df.orderBy("id").select("id", "metrics").collect()
            self.assertEqual(len(rows), 2)
            r1 = rows[0].asDict()
            r2 = rows[1].asDict()
            self.assertEqual(r1["id"], 1)
            self.assertEqual(r1["metrics"], {"cpu": 0.5, "mem": 1024.0})
            self.assertEqual(r2["id"], 2)
            self.assertEqual(r2["metrics"], {"cpu": 0.8, "disk": 500.0})

    def test_spark_read_with_regex_and_path_regex(self):
        if not _HAVE_SPARK:
            self.skipTest("pyspark not available")

        with tempfile.TemporaryDirectory() as tmpdir:
            f1 = os.path.join(tmpdir, "partnerA_orders_2026.csv")
            f2 = os.path.join(tmpdir, "partnerB_orders_2026.csv")

            with open(f1, "w") as fp:
                fp.write("Order_Num,Cust_ID,Amount\n101,cust_1,150.50\n")
            with open(f2, "w") as fp:
                fp.write("txn_id,client_key,total\n201,cust_2,300.00\n")

            mappings = [
                {
                    "path_regex": r".*partnerA_orders_.*\.csv$",
                    "regex": r"(?i)^order[_\s]?(num|id)$",
                    "canonical": "order_id",
                    "type": "bigint",
                    "required": True,
                },
                {
                    "path_regex": r".*partnerA_orders_.*\.csv$",
                    "regex": r"(?i)^cust[_\s]?id$",
                    "canonical": "customer_id",
                    "type": "string",
                },
                {
                    "path_regex": r".*partnerA_orders_.*\.csv$",
                    "header": "Amount",
                    "canonical": "amount",
                    "type": "double",
                },
                {
                    "path_regex": r".*partnerB_orders_.*\.csv$",
                    "regex": r"(?i)^txn[_\s]?id$",
                    "canonical": "order_id",
                    "type": "bigint",
                    "required": True,
                },
                {
                    "path_regex": r".*partnerB_orders_.*\.csv$",
                    "header": "client_key",
                    "canonical": "customer_id",
                    "type": "string",
                },
                {
                    "path_regex": r".*partnerB_orders_.*\.csv$",
                    "header": "total",
                    "canonical": "amount",
                    "type": "double",
                },
            ]

            headers = {
                f1: ["Order_Num", "Cust_ID", "Amount"],
                f2: ["txn_id", "client_key", "total"],
            }
            groups, rejects = plan_groups([f1, f2], mappings, headers)
            self.assertEqual(rejects, [])
            self.assertEqual(len(groups), 2)

            df = read_groups(self.spark, groups, tracking_column="_source_file")
            rows = df.orderBy("order_id").collect()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["order_id"], 101)
            self.assertEqual(rows[0]["customer_id"], "cust_1")
            self.assertEqual(rows[0]["amount"], 150.50)
            self.assertIn("partnerA_orders_2026.csv", rows[0]["_source_file"])

            self.assertEqual(rows[1]["order_id"], 201)
            self.assertEqual(rows[1]["customer_id"], "cust_2")
            self.assertEqual(rows[1]["amount"], 300.00)
            self.assertIn("partnerB_orders_2026.csv", rows[1]["_source_file"])

    def test_row_index_normalization(self):
        self.assertIsNone(normalize_row_index(None))
        self.assertIsNone(normalize_row_index(False))
        self.assertIsNone(normalize_row_index(""))
        self.assertEqual(normalize_row_index(True), ("_row_index", 0))
        self.assertEqual(normalize_row_index("row_id"), ("row_id", 0))
        self.assertEqual(normalize_row_index({"name": "line_no", "start": 1}), ("line_no", 1))
        self.assertEqual(normalize_row_index({"name": "idx"}), ("idx", 0))

    def test_check_read_row_index(self):
        path = "s3://b/f.csv"
        for value in (True, "row_id", {"name": "line_no", "start": 1}):
            self.assertEqual(check_read({"path": path, "row_index": value}), [])
        self.assertTrue(any("plain identifier" in p for p in check_read({"path": path, "row_index": "bad name!"})))
        self.assertTrue(
            any(
                "non-negative integer" in p
                for p in check_read({"path": path, "row_index": {"name": "line_no", "start": -1}})
            )
        )
        probs = check_read(
            {
                "path": path,
                "row_index": "order_id",
                "columns": [{"prefix": "s3://b/", "header": "oid", "canonical": "order_id", "type": "int"}],
            }
        )
        self.assertTrue(any("collides with mapped canonical column" in p for p in probs))

    def test_spark_read_with_row_index(self):
        if not _HAVE_SPARK:
            self.skipTest("pyspark not available")

        with tempfile.TemporaryDirectory() as tmpdir:
            f1 = os.path.join(tmpdir, "orders_01.csv")
            f2 = os.path.join(tmpdir, "orders_02.csv")

            with open(f1, "w") as fp:
                fp.write("id,item\n10,apple\n20,banana\n")
            with open(f2, "w") as fp:
                fp.write("id,item\n30,cherry\n40,date\n50,elderberry\n")

            g1 = Group(
                prefix="",
                header=["id", "item"],
                columns=[("id", "id", "int"), ("item", "item", "string")],
                files=[f1],
                format="csv",
            )
            g2 = Group(
                prefix="",
                header=["id", "item"],
                columns=[("id", "id", "int"), ("item", "item", "string")],
                files=[f2],
                format="csv",
            )

            # 1. Test 0-indexed row_id string alias
            df0 = read_groups(self.spark, [g1, g2], row_index="row_id")
            rows0 = df0.orderBy("_source_file", "row_id").collect()
            self.assertEqual(len(rows0), 5)
            # f1 rows: 0, 1
            f1_rows = [r for r in rows0 if "orders_01.csv" in r["_source_file"]]
            self.assertEqual([r["row_id"] for r in f1_rows], [0, 1])
            self.assertEqual([r["id"] for r in f1_rows], [10, 20])
            # f2 rows: 0, 1, 2
            f2_rows = [r for r in rows0 if "orders_02.csv" in r["_source_file"]]
            self.assertEqual([r["row_id"] for r in f2_rows], [0, 1, 2])
            self.assertEqual([r["id"] for r in f2_rows], [30, 40, 50])

            # 2. Test 1-indexed custom line_no dict option
            df1 = read_groups(self.spark, [g1, g2], row_index={"name": "line_no", "start": 1})
            rows1 = df1.orderBy("_source_file", "line_no").collect()
            f1_rows1 = [r for r in rows1 if "orders_01.csv" in r["_source_file"]]
            self.assertEqual([r["line_no"] for r in f1_rows1], [1, 2])
            f2_rows1 = [r for r in rows1 if "orders_02.csv" in r["_source_file"]]
            self.assertEqual([r["line_no"] for r in f2_rows1], [1, 2, 3])


class Vocabulary(unittest.TestCase):
    def test_valid(self):
        check_vocabulary(mapping_rows(), vocabulary_rows())

    def test_path_regex_and_regex_validations(self):
        # Valid path_regex and regex
        check_vocabulary(
            [{"path_regex": r".*/orders_.*\.csv$", "regex": r"(?i)^order.*", "canonical": "amo_id", "type": "string"}],
            vocabulary_rows(),
        )

        # Invalid path_regex syntax
        with self.assertRaises(IngestError) as caught:
            check_vocabulary(
                [{"path_regex": "[invalid(", "header": "h", "canonical": "amo_id", "type": "string"}], vocabulary_rows()
            )
        self.assertIn("invalid path_regex", str(caught.exception))

        # Invalid header regex syntax
        with self.assertRaises(IngestError) as caught:
            check_vocabulary(
                [{"prefix": "p", "regex": "*bad", "canonical": "amo_id", "type": "string"}], vocabulary_rows()
            )
        self.assertIn("invalid regex", str(caught.exception))

        # A plain list now applies to every file without a selector.
        check_vocabulary([{"header": "h", "canonical": "amo_id", "type": "string"}], vocabulary_rows())

        # Missing both header and regex
        with self.assertRaises(IngestError) as caught:
            check_vocabulary([{"prefix": "p", "canonical": "amo_id", "type": "string"}], vocabulary_rows())
        self.assertIn("must specify either header or regex", str(caught.exception))

    def test_unknown_name_and_type_mismatch(self):
        rows = [
            *mapping_rows()[:1],
            {"prefix": "p", "header": "h", "canonical": "std_event_1", "type": "int", "required": False},
            {"prefix": "p", "header": "h2", "canonical": "revenue", "type": "int", "required": False},
        ]
        with self.assertRaises(IngestError) as caught:
            check_vocabulary(rows, vocabulary_rows())
        self.assertIn("std_event_1", str(caught.exception))
        self.assertIn("vocabulary says double", str(caught.exception))

    def test_injection_in_type_or_name(self):
        for bad in ({"canonical": "x; DROP", "type": "string"}, {"canonical": "x", "type": "string) FROM t --"}):
            row = dict({"prefix": "p", "header": "h", "required": False}, **bad)
            with self.assertRaises(IngestError):
                check_vocabulary([row], None)


class Header(unittest.TestCase):
    def test_header_with_bom_and_quotes(self):
        self.assertEqual(parse_header('\ufeff"AMO ID (evar8)", revenue ,x'), ["AMO ID (evar8)", "revenue", "x"])
        self.assertEqual(parse_header("a\tb", "\t"), ["a", "b"])


class PathPatternAndDictMappings(unittest.TestCase):
    def test_normalize_columns_flat_list(self):
        rows = [
            {"prefix": "s3://b/p1/", "header": "h1", "canonical": "c1", "type": "string"},
            {"prefix": "s3://b/p2/", "header": "h2", "canonical": "c2", "type": "bigint"},
        ]
        norm, selector = normalize_columns(rows)
        self.assertEqual(norm, rows)
        self.assertIsNone(selector)

    def test_normalize_columns_dict_of_lists(self):
        cols = {
            "by": "partner",
            "alpha": [
                {"header": "Order_Num", "canonical": "order_id", "type": "bigint", "required": True},
                {"regex": "(?i)^amt$", "canonical": "amount", "type": "double"},
            ],
            "beta": [
                {"header": "txn_id", "canonical": "order_id", "type": "bigint"},
            ],
        }
        norm, selector = normalize_columns(cols)
        self.assertEqual(selector, "partner")
        self.assertEqual(len(norm), 3)
        alpha_rules = [r for r in norm if r.get("schema_key") == "alpha"]
        self.assertEqual(len(alpha_rules), 2)
        self.assertEqual(alpha_rules[0]["canonical"], "order_id")
        self.assertTrue(alpha_rules[0]["required"])
        self.assertEqual(alpha_rules[1]["regex"], "(?i)^amt$")

    def test_normalize_columns_dict_of_dicts(self):
        cols = {
            "by": "{{ client }}/{{ feed }}",
            "alpha/orders": {
                "order_id": {"header": "Order_Num", "type": "bigint", "required": True},
                "amount": {"header": "AmountUSD", "type": "double"},
            },
        }
        norm, selector = normalize_columns(cols)
        self.assertEqual(selector, "{{ client }}/{{ feed }}")
        self.assertEqual(len(norm), 2)
        self.assertEqual(norm[0]["schema_key"], "alpha/orders")
        self.assertEqual(norm[0]["canonical"], "order_id")
        self.assertEqual(norm[0]["header"], "Order_Num")
        self.assertEqual(norm[0]["type"], "bigint")
        self.assertTrue(norm[0]["required"])

    def test_check_vocabulary_with_dict(self):
        vocab = [
            {"name": "order_id", "type": "bigint"},
            {"name": "amount", "type": "double"},
        ]
        valid_cols = {
            "by": "client",
            "alpha": [
                {"header": "Order_Num", "canonical": "order_id", "type": "bigint"},
                {"header": "Amount", "canonical": "amount", "type": "double"},
            ],
        }
        check_vocabulary(valid_cols, vocab)

        invalid_cols = {
            "alpha": [
                {"header": "Order_Num", "canonical": "unknown_col", "type": "bigint"},
            ],
        }
        with self.assertRaises(IngestError) as ctx:
            check_vocabulary(invalid_cols, vocab)
        self.assertIn("'unknown_col' (target alpha) is not in the controlled vocabulary", str(ctx.exception))

    def test_plan_ingest_path_pattern(self):
        files = [
            "s3://landing/partners/alpha/dt=2026-03-01/orders.csv",
            "s3://landing/partners/beta/dt=2026-03-01/data.tsv",
        ]
        headers = {
            files[0]: ["Order_Num", "AmountUSD"],
            files[1]: ["transaction_id", "total"],
        }
        formats = {
            files[0]: "csv",
            files[1]: "tsv",
        }
        columns = {
            "by": "partner",
            "alpha": [
                {"header": "Order_Num", "canonical": "order_id", "type": "bigint"},
                {"header": "AmountUSD", "canonical": "amount", "type": "double"},
            ],
            "beta": [
                {"header": "transaction_id", "canonical": "order_id", "type": "bigint"},
                {"header": "total", "canonical": "amount", "type": "double"},
            ],
        }
        pattern = r"partners/(?P<partner>[^/]+)/"
        groups, rejects, reports = plan_ingest(
            files,
            mappings=columns,
            headers=headers,
            formats=formats,
            path_pattern=pattern,
            inject_path_vars=True,
        )
        self.assertEqual(len(rejects), 0)
        self.assertEqual(len(groups), 2)
        # Check path vars spec attached
        self.assertIsNotNone(groups[0].path_vars_spec)
        stripped_pat, group_idx_map = groups[0].path_vars_spec
        self.assertEqual(stripped_pat, r"partners/([^/]+)/")
        self.assertEqual(group_idx_map, {"partner": 1})

        # Test serialization roundtrip of plan with path_vars_spec
        raw_plan = serialize_plan(groups, rejects, reports)
        g2, _rej2, _rep2 = deserialize_plan(raw_plan)
        self.assertEqual(len(g2), 2)
        self.assertEqual(g2[0].path_vars_spec, groups[0].path_vars_spec)

    def test_plan_ingest_path_pattern_rejections(self):
        files = [
            "s3://landing/other/gamma/orders.csv",  # does not match regex
            "s3://landing/partners/unknown/orders.csv",  # matches regex but unknown partner
        ]
        headers = {f: ["id"] for f in files}
        columns = {
            "by": "partner",
            "alpha": [{"header": "id", "canonical": "order_id", "type": "bigint"}],
        }
        pattern = r"partners/(?P<partner>[^/]+)/"
        groups, rejects, _reports = plan_ingest(
            files,
            mappings=columns,
            headers=headers,
            path_pattern=pattern,
        )
        self.assertEqual(len(groups), 0)
        self.assertEqual(len(rejects), 2)
        self.assertIn("does not match path_pattern", rejects[0].reason)
        self.assertIn("unknown schema key 'unknown'", rejects[1].reason)

    def test_read_groups_with_injected_path_vars(self):
        if not _HAVE_SPARK:
            return
        spark = SparkSession.builder.master("local[1]").appName("test-path-pattern").getOrCreate()
        with tempfile.TemporaryDirectory() as tmpdir:
            alpha_dir = os.path.join(tmpdir, "partners", "alpha")
            os.makedirs(alpha_dir, exist_ok=True)
            f1 = os.path.join(alpha_dir, "orders.csv")
            with open(f1, "w") as fp:
                fp.write("Order_Num,AmountUSD\n101,55.5\n")

            headers = {f1: ["Order_Num", "AmountUSD"]}
            columns = {
                "by": "partner",
                "alpha": [
                    {"header": "Order_Num", "canonical": "order_id", "type": "bigint"},
                    {"header": "AmountUSD", "canonical": "amount", "type": "double"},
                ],
            }
            pattern = r"partners/(?P<partner>[^/]+)/"
            groups, _rejects, _reports = plan_ingest(
                [f1],
                mappings=columns,
                headers=headers,
                formats={f1: "csv"},
                path_pattern=pattern,
                inject_path_vars=True,
            )
            df = read_groups(spark, groups)
            self.assertIn("partner", df.columns)
            self.assertIn("order_id", df.columns)
            self.assertIn("amount", df.columns)
            row = df.collect()[0]
            self.assertEqual(row["partner"], "alpha")
            self.assertEqual(row["order_id"], 101)
            self.assertEqual(row["amount"], 55.5)


if __name__ == "__main__":
    unittest.main()
