import unittest

from streamwright.spark.storage import naming
from streamwright.spark.storage.exports import ExportError, splits_from_dirs


class Dirs(unittest.TestCase):
    def test_splits(self):
        files = ["__sw_p0=acme/__sw_p1=2026-10-15/part-0.csv.gz", "__sw_p0=b%20c/__sw_p1=2026-10-16/part-1.csv.gz"]
        found = splits_from_dirs(files, ["client_id", "event_date"])
        self.assertEqual(found[("acme", "2026-10-15")][0], {"client_id": "acme", "event_date": "2026-10-15"})
        self.assertEqual(found[("b c", "2026-10-16")][0]["client_id"], "b c")

    def test_null_split_value(self):
        with self.assertRaises(ExportError):
            splits_from_dirs(["__sw_p0=__HIVE_DEFAULT_PARTITION__/part-0.csv"], ["client_id"])

    def test_unexpected_layout(self):
        with self.assertRaises(ExportError):
            splits_from_dirs(["other=1/part-0.csv"], ["client_id"])
        with self.assertRaises(ExportError):
            splits_from_dirs(["part-0.csv"], ["client_id"])

    def test_names_from_listed_splits(self):
        files = ["__sw_p0=acme/__sw_p1=2026-10-15/part-0.csv.gz", "__sw_p0=zed/__sw_p1=2026-10-15/part-1.csv.gz"]
        found = splits_from_dirs(files, ["client_id", "event_date"])
        cols = ["client_id", "event_date"]
        file_t = naming.parse_template("{client_id}_{event_date:%Y%m%d}_{today}.csv.gz", cols)
        layout = naming.parse_template("{client_id}/dt={event_date:%Y-%m-%d}", cols, True)
        names = naming.plan_names(file_t, layout, [(k, v[0]) for k, v in sorted(found.items())],
                                  "2026-10-17T03:00:00Z", "b1")
        self.assertEqual(names[("acme", "2026-10-15")], "acme/dt=2026-10-15/acme_20261015_20261017.csv.gz")


if __name__ == "__main__":
    unittest.main()
