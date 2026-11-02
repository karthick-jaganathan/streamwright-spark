"""Path containment and secret-safe session configuration use provider hooks."""

import pytest

from streamwright.core.sdk.contract import Field, Operation, OperationKind
from streamwright.spark.contracts import Connection
from streamwright.spark.engine.session import hadoop_settings
from streamwright.spark.io.file_rules import check_read
from streamwright.spark.io.files import FileLocation, spark_options
from streamwright.spark.storage.paths import StorageError, check_path, spark_text


class Store:
    def hadoop_settings(self, name, auth):
        return {"fs.store.region": auth["region"]} if auth.get("region") else {}


def test_paths_only_accept_declared_url_form():
    assert check_path("store://bucket/inside/file", ["store://bucket/"]) == "store://bucket/inside/file"
    assert FileLocation(("store://bucket/",), "store", "hadoop").to_hadoop("store://bucket/inside/file") == (
        "hadoop://bucket/inside/file"
    )
    for outside in ("other://bucket/file", "store://other/file", "store://bucket/../outside", ""):
        with pytest.raises(StorageError):
            check_path(outside, ["store://bucket/"])
    assert (spark_text(True), spark_text(False), spark_text(5)) == ("true", "false", "5")


def test_session_never_copies_credentials_into_spark_conf(definition_adapter):
    adapter = definition_adapter(
        "store",
        Store(),
        operations=(Operation(service="object", method="read", kind=OperationKind.READ),),
        connection=(Field("password", "string", secret=True), Field("region", "string")),
    )
    with pytest.raises(ValueError, match="credentials cannot be placed in Spark conf"):
        hadoop_settings(
            {"source": Connection("source", "store", {"password": "secret", "region": "secret"})}, {"store": adapter}
        )
    assert hadoop_settings({"source": Connection("source", "store", {"region": "west"})}, {"store": adapter}) == {
        "fs.store.region": "west"
    }


def test_reader_options_translate_known_names_and_forward_vendor_options():
    options = {"header": True, "delimiter": ";", "nullstr": "NA", "dateformat": "yyyy-MM-dd", "multiLine": True}
    assert spark_options("csv", options) == {
        "header": "true",
        "sep": ";",
        "nullValue": "NA",
        "dateFormat": "yyyy-MM-dd",
        "multiLine": "true",
    }
    assert spark_options("tsv", {}) == {"sep": "\t"}
    # a vendor option (Spark's own name, or any other) is forwarded verbatim to DataFrameReader/Writer.option
    assert spark_options("csv", {"sep": ";", "mergeSchema": "true"}) == {"sep": ";", "mergeSchema": "true"}
    problems = check_read({"path": "a.csv", "options": {"skip": 1, "nullstr": ["", "NA"]}})
    assert [problem.split(" (")[0] for problem in problems] == [
        "option 'nullstr' is a single text on the spark engine",
    ]
