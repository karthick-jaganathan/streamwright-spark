import os
import tempfile
import unittest

from streamwright.core.jobs import BatchContext, naming
from streamwright.spark.engine.runtime import StepRuntime
from streamwright.spark.io.files import FileLocation, write_files
from streamwright.spark.sdk import SparkContext
from streamwright.spark.storage.exports import ExportError, splits_from_dirs

try:
    from pyspark.sql import SparkSession

    _HAVE_SPARK = True
except ImportError:
    _HAVE_SPARK = False


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
        names = naming.plan_names(
            file_t, layout, [(k, v[0]) for k, v in sorted(found.items())], "2026-10-17T03:00:00Z", "b1"
        )
        self.assertEqual(names[("acme", "2026-10-15")], "acme/dt=2026-10-15/acme_20261015_20261017.csv.gz")


@unittest.skipUnless(_HAVE_SPARK, "pyspark is not installed")
class SplitExportEndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test_split_export")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_split_by_export_publishes_one_file_per_split(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            frame = self.spark.createDataFrame(
                [("acme", 10), ("acme", 20), ("beta", 30)], ["client_id", "amount"]
            )
            ctx = BatchContext("b-1", 1, "2026-10-17T03:00:00Z", "job", 1, "store-id", {})
            runtime = StepRuntime(self.spark, ctx, {}, {}, {}, limits={})
            context = SparkContext(runtime, "orders", {})
            location = FileLocation((tmpdir,), "file", "file")
            receipt = write_files(
                context,
                frame,
                {"path": "out", "format": "csv", "split_by": ["client_id"],
                 "file_name": "{client_id}.csv", "options": {"header": True}},
                location=location,
            )
            assert sorted(os.path.basename(f) for f in receipt.files) == ["acme.csv", "beta.csv"]
            assert receipt.rows == 3
            for name in ("acme.csv", "beta.csv"):
                assert os.path.exists(os.path.join(tmpdir, "out", name))

    def test_file_name_without_split_by_publishes_one_named_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            frame = self.spark.createDataFrame([("acme", 10), ("beta", 30)], ["client_id", "amount"])
            ctx = BatchContext("b-1", 1, "2026-10-17T03:00:00Z", "job", 1, "store-id", {})
            context = SparkContext(StepRuntime(self.spark, ctx, {}, {}, {}, limits={}), "orders", {})
            receipt = write_files(
                context,
                frame,
                {"path": "out", "format": "csv", "layout": "{today:%Y/%m}", "file_name": "orders_{today}.csv",
                 "options": {"header": True}},
                location=FileLocation((tmpdir,), "file", "file"),
            )
            assert [os.path.relpath(f, os.path.join(tmpdir, "out")) for f in receipt.files] == [
                "2026/10/orders_20261017.csv"]
            assert receipt.rows == 2


if __name__ == "__main__":
    unittest.main()
