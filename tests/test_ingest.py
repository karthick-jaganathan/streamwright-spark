import unittest

from streamwright.spark.storage.ingest import IngestError, check_vocabulary, parse_header, plan_groups

MAP = [
    {"prefix": "s3a://l/a/", "header": "timestamp", "canonical": "event_ts", "type": "timestamp", "required": True},
    {"prefix": "s3a://l/a/", "header": "AMO ID (evar8)", "canonical": "amo_id", "type": "string", "required": False},
    {"prefix": "s3a://l/a/", "header": "revenue", "canonical": "revenue", "type": "double", "required": False},
    {"prefix": "s3a://l/b/", "header": "timestamp", "canonical": "event_ts", "type": "timestamp", "required": True},
    {"prefix": "s3a://l/b/", "header": "AMO_ID (evar18)", "canonical": "amo_id", "type": "string", "required": False},
    {"prefix": "s3a://l/b/", "header": "Revenue", "canonical": "revenue", "type": "double", "required": False},
    {"prefix": "s3a://l/b/vip/", "header": "ts", "canonical": "event_ts", "type": "timestamp", "required": True},
]
VOCAB = [{"name": "event_ts", "type": "timestamp"}, {"name": "amo_id", "type": "string"},
         {"name": "revenue", "type": "double"}]


class Groups(unittest.TestCase):
    def test_two_clients_two_headers(self):
        files = ["s3a://l/a/1.csv", "s3a://l/a/2.csv", "s3a://l/b/1.csv"]
        headers = {"s3a://l/a/1.csv": ["timestamp", "AMO ID (evar8)", "revenue", "junk"],
                   "s3a://l/a/2.csv": ["timestamp", "AMO ID (evar8)", "revenue", "junk"],
                   "s3a://l/b/1.csv": ["timestamp", "AMO_ID (evar18)", "Revenue"]}
        groups, rejects = plan_groups(files, MAP, headers)
        self.assertEqual(rejects, [])
        self.assertEqual([len(g.files) for g in groups], [2, 1])
        self.assertEqual([c[1] for c in groups[0].columns], ["event_ts", "amo_id", "revenue"])
        self.assertEqual([c[0] for c in groups[1].columns], ["timestamp", "AMO_ID (evar18)", "Revenue"])

    def test_longest_prefix_wins(self):
        groups, rejects = plan_groups(["s3a://l/b/vip/1.csv"], MAP, {"s3a://l/b/vip/1.csv": ["ts", "x"]})
        self.assertEqual(rejects, [])
        self.assertEqual(groups[0].prefix, "s3a://l/b/vip/")

    def test_rejects_do_not_fail_other_files(self):
        files = ["s3a://l/a/1.csv", "s3a://l/a/2.csv", "s3a://l/zzz/1.csv", "s3a://l/a/3.csv"]
        headers = {"s3a://l/a/1.csv": ["timestamp", "revenue"], "s3a://l/a/2.csv": ["revenue"],
                   "s3a://l/zzz/1.csv": ["x"], "s3a://l/a/3.csv": []}
        groups, rejects = plan_groups(files, MAP, headers)
        self.assertEqual(len(groups), 1)
        reasons = dict((r.path, r.reason) for r in rejects)
        self.assertIn("missing required", reasons["s3a://l/a/2.csv"])
        self.assertEqual(reasons["s3a://l/zzz/1.csv"], "no column mapping for this path")
        self.assertIn("header", reasons["s3a://l/a/3.csv"])

    def test_underscore_in_prefix_is_literal(self):
        rows = [{"prefix": "s3a://l/a_b/", "header": "t", "canonical": "event_ts", "type": "timestamp", "required": False}]
        _, rejects = plan_groups(["s3a://l/aXb/1.csv"], rows, {"s3a://l/aXb/1.csv": ["t"]})
        self.assertEqual(rejects[0].reason, "no column mapping for this path")

    def test_two_columns_to_one_name(self):
        rows = MAP[:3] + [{"prefix": "s3a://l/a/", "header": "rev2", "canonical": "revenue", "type": "double", "required": False}]
        _, rejects = plan_groups(["s3a://l/a/1.csv"], rows, {"s3a://l/a/1.csv": ["timestamp", "revenue", "rev2"]})
        self.assertIn("both map", rejects[0].reason)

    def test_group_limit(self):
        files = ["s3a://l/a/%d.csv" % i for i in range(3)]
        headers = dict((f, ["timestamp"] + ["revenue"] * i) for i, f in enumerate(files))
        headers["s3a://l/a/2.csv"] = ["timestamp", "revenue", "AMO ID (evar8)"]
        with self.assertRaises(IngestError):
            plan_groups(files, MAP, headers, max_groups=1)


class Vocabulary(unittest.TestCase):
    def test_valid(self):
        check_vocabulary(MAP, VOCAB)

    def test_unknown_name_and_type_mismatch(self):
        rows = MAP[:1] + [{"prefix": "p", "header": "h", "canonical": "std_event_1", "type": "int", "required": False},
                          {"prefix": "p", "header": "h2", "canonical": "revenue", "type": "int", "required": False}]
        with self.assertRaises(IngestError) as caught:
            check_vocabulary(rows, VOCAB)
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


if __name__ == "__main__":
    unittest.main()
