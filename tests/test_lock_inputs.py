"""Version locks derive dependencies from adapter facts, not engine vendor tables."""

import pytest

from streamwright.core.jobs.inputs import InputError, parse_set
from streamwright.core.sdk.contract import Operation, OperationKind
from streamwright.spark.runtime import lock


class TableAdapter:
    packages = ("table-sdk",)
    jars = (("org.example:table", "ship", "1.2.3"),)


def installed(name):
    return {"table-sdk": "2.0", "streamwright-spark": "0.1.0"}.get(name)


def test_lock_uses_adapter_dependencies_and_install_hint(definition_adapter):
    adapter = definition_adapter(
        "table", TableAdapter(), operations=(Operation(service="database", method="put", kind=OperationKind.WRITE),)
    )
    built = lock.build_lock("job", [adapter], "3.5.3", installed=installed)
    assert built["packages"]["table-sdk"] == "2.0"
    assert built["jars"] == [{"coordinates": "org.example:table:1.2.3", "mode": "ship"}]
    assert lock.check_lock(built, "3.5.3", installed=installed) == []
    with pytest.raises(lock.LockError, match="streamwright connectors install table"):
        lock.build_lock("job", [adapter], "3.5.3", installed=lambda name: None)


def test_set_values_reject_missing_equal_sign():
    assert parse_set(["a=1", "b=x=y"]) == {"a": "1", "b": "x=y"}
    with pytest.raises(InputError):
        parse_set(["no-equals"])
