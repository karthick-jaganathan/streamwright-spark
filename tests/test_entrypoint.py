"""Cluster bootstrap unpacks shipped wheels before importing the driver."""

import sys
import zipfile
from importlib.metadata import entry_points

from streamwright.core.jobs.engine import JobEngine, load_job_engine
from streamwright.spark.entrypoint import _bootstrap_wheels


def test_cluster_bootstrap_exposes_wheel_namespace(monkeypatch, tmp_path):
    wheel = tmp_path / "shipped.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("streamwright/plugin/resource.txt", "installed")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", [*list(sys.path), str(wheel)])
    _bootstrap_wheels()
    assert (tmp_path / ".wheels/streamwright/plugin/resource.txt").read_text() == "installed"
    assert str(tmp_path / ".wheels") in sys.path


def test_spark_job_kind_entry_point_exposes_explicit_engine():
    entries = [entry for entry in entry_points(group="streamwright.job_kinds") if entry.name == "SparkJob"]
    assert len(entries) == 1
    engine = load_job_engine("SparkJob")
    assert isinstance(engine, JobEngine)
    assert engine.kind == "SparkJob"
    assert not hasattr(engine, "connector_group")
    assert engine.json_schema()["properties"]["kind"]["enum"] == ["SparkJob"]
    assert engine.guide()
    assert engine.describe_connector("not-installed") is None
