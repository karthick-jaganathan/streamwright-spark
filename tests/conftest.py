"""Definition-backed adapters used by Spark contract tests."""

import pytest

from streamwright.core.sdk.contract import ConnectorDefinition
from streamwright.spark.runtime.definition import DefinedAdapter


@pytest.fixture
def definition_adapter():
    def create(name, adapter, *, operations, connection=(), family="test"):
        definition = ConnectorDefinition(
            name=name,
            title=name,
            family=family,
            connection=connection,
            operations=operations,
            adapters={"spark": "tests.conftest:UnusedAdapter"},
        )
        return DefinedAdapter(definition, adapter)

    return create
