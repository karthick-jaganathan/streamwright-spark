#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

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


def test_connectors_run_as_call_steps_are_pinned_too():
    built = lock.build_lock("job", [], "3.5.3", driver_packages=["streamwright-google-ads"],
                            installed=lambda name: "1.4" if name == "streamwright-google-ads" else "0.1")
    assert built["packages"] == {"streamwright-google-ads": "1.4", "streamwright-spark": "0.1"}
    with pytest.raises(lock.LockError, match="pip install streamwright-google-ads"):
        lock.build_lock("job", [], "3.5.3", driver_packages=["streamwright-google-ads"], installed=lambda name: None)


def test_set_values_reject_missing_equal_sign():
    assert parse_set(["a=1", "b=x=y"]) == {"a": "1", "b": "x=y"}
    with pytest.raises(InputError):
        parse_set(["no-equals"])
