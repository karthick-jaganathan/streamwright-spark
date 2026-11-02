"""SparkJob implementation of the shared job-engine protocol."""

from streamwright.spark.cli import main as spark_main
from streamwright.spark.docs import guide as spark_guide
from streamwright.spark.job import validate, validate_text
from streamwright.spark.runtime.registry import Registry, RegistryError
from streamwright.spark.schema import json_schema as spark_schema

__all__ = ["JOB_ENGINE"]


class SparkJobEngine:
    kind = "SparkJob"

    def main(self, argv=None):
        return spark_main(argv)

    def validate_job(self, job, *, overlays=(), config=None, strict=False):
        return validate(job, overlays=overlays, config=config, strict=strict)

    def validate_job_text(self, text, *, overlays=(), config=None, strict=False):
        return validate_text(text, overlays=overlays, config=config, strict=strict)

    def json_schema(self):
        return spark_schema()

    def guide(self, section=None):
        return spark_guide(section)

    def describe_connector(self, name):
        try:
            return Registry().load(name).describe()
        except RegistryError:
            return None


JOB_ENGINE = SparkJobEngine()
