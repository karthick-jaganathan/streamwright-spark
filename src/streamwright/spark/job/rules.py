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

"""Spark-only rule tables and adapter hooks for the shared jobs checker."""

from types import MappingProxyType

from streamwright.core.jobs import JobRules
from streamwright.core.jobs.apply.log import OUTPUT_LOG_FIELD
from streamwright.core.jobs.apply.rules import check_apply, is_apply_output
from streamwright.core.jobs.calls import call_problems, connection_problems, partition_problems
from streamwright.core.jobs.check.rules import NAME_PATTERN, RESERVED_NAMES, input_field, install_hint
from streamwright.core.jobs.fields import Field
from streamwright.core.jobs.grammar import (
    APPLY_FIELD,
    ASYNC_JOB_FIELD,
    CONNECTION_GROUPS,
    OPERATION_FIELD,
    POLICY_FIELD,
)
from streamwright.core.jobs.locking import STATE_FIELDS
from streamwright.core.jobs.log_labels import LOG_FIELD
from streamwright.core.jobs.lookups.rules import FAN_OUT_FIELD, LOOKUPS_FIELD
from streamwright.core.jobs.policy import OUTPUT_POLICY_KEYS, STEP_POLICY_KEYS
from streamwright.core.jobs.refs import references, walk_strings
from streamwright.core.jobs.step_log import LOG_KEY, STEP_LOG_FIELD
from streamwright.core.jobs.write_rules import OutputSpec, WriteTarget, warn_merge_scope
from streamwright.core.runtime import components
from streamwright.core.sdk.contract import check_arguments, check_connection
from streamwright.spark.job.runtime_rules import check_platform, check_runtime
from streamwright.spark.job.vocabulary import BUILTIN_REFERENCES, LIMITS_FIELD, RECORDS_VIEW, storage_level_name
from streamwright.spark.plan.analysis import dependencies, sql_parameters

__all__ = ["SparkRules"]

_TEXT = Field("string")
# a source-connector call step (contract v1.2 §6a engine reach): an `operation:`, run on the driver
_CALL_FIELDS = MappingProxyType({
    # one call per distinct value of an earlier step's column, read as {{ partition.<name> }} (core jobs.calls)
    "partitions": Field("array", items=Field("object", fields={"from": Field("string", required=True),
                                                               "field": Field("string", required=True),
                                                               "name": _TEXT})),
})
_REPLAY_SAFE_MODES = ("overwrite", "merge", "delete")
_BUILTINS = frozenset(BUILTIN_REFERENCES)


def _object(**fields):
    return Field("object", fields=fields)


def _contract_issue(issue):
    return f"{issue.path}: {issue.message}" if issue.path else issue.message


class SparkRules(JobRules):
    kind = "SparkJob"
    builtin_references = _BUILTINS
    reserved_names = RESERVED_NAMES | {RECORDS_VIEW}
    step_reference_keys = frozenset(("arguments",))
    source_reference_keys = frozenset(("request", "paginator"))  # rendered by the source engine on a call step
    step_kinds = MappingProxyType(
        {
            "sql": ("name", "sql", "persist", "checkpoint", "view", LOG_KEY),
            "call": ("name", "connection", "service", "method", "arguments", "sql", "persist", "checkpoint", "view",
                     LOG_KEY),
            "source_call": ("name", "connection", "service", "method", "arguments", "request", "async_job",
                            "partitions", "sql", "persist", "checkpoint", "view", LOG_KEY),
            "source": ("name", "source", "stream", "config", "partitions", "sql", "persist", "checkpoint", "view",
                       LOG_KEY),
        }
    )

    def __init__(self, adapters, launchers=None, connectors=None):
        self.adapters = dict(adapters)
        self.launchers = launchers
        self.connectors = set(connectors if connectors is not None else components.available())
        self.builtin_references = _BUILTINS | frozenset(
            token for adapter in self.adapters.values() for token in adapter.template_tokens()
        )

    @staticmethod
    def _definition_field(field):
        # Core checks fields and references; the job shape checker must not reject templated values first.
        return Field(description=field.description)

    def manifest_field(self):
        item = {
            "name": Field("string", required=True, pattern=NAME_PATTERN),
            "connection": _TEXT,
            "operation": OPERATION_FIELD,
            "sql": _TEXT,
            "persist": _TEXT,
            "checkpoint": Field("boolean"),
            "view": Field("string", enum=("local", "global")),
            "partitions": _CALL_FIELDS["partitions"],
            "apply": APPLY_FIELD,
            "policy": POLICY_FIELD,
        }
        runtime = _object(
            state=_object(connection=_TEXT, schema=_TEXT, sqlite=_TEXT, ttl_seconds=Field("integer", minimum=1),
                          **STATE_FIELDS),
            spark=_object(app_name=_TEXT, version=_TEXT, conf=Field("object")),
            platform=Field("object"),
            limits=LIMITS_FIELD,
            env=Field("object"),
            secrets=Field("object"),
            artifacts=Field("object"),
            fan_out=FAN_OUT_FIELD,
            log=LOG_FIELD,
        )
        variants = {}
        for provider, adapter in self.adapters.items():
            variants[provider] = {field.name: self._definition_field(field) for field in adapter.definition.connection}
        connection = Field(
            "object",
            fields={"provider": Field("string", required=True), **CONNECTION_GROUPS},
            open=True,
            discriminator="provider",
            variants=variants,
        )
        return _object(
            kind=Field("string", required=True, enum=("SparkJob",)),
            version=Field(("integer", "string")),
            name=Field("string", required=True, pattern=NAME_PATTERN),
            description=_TEXT,
            spec=_object(config=Field("object", values=input_field()), secrets=Field("object", values=input_field()),
                         lookups=LOOKUPS_FIELD),
            connections=Field("object", required=True, min_items=1, values=connection),
            steps=Field("array", items=Field("object", fields=dict(
                item, partitions=_CALL_FIELDS["partitions"], async_job=ASYNC_JOB_FIELD, log=STEP_LOG_FIELD,
                source=Field("string", description="a StreamWright source folder (relative to the job's folder, "
                                                   "local or remote)"),
                stream=Field("string"), config=Field("object")))),
            outputs=Field(
                "array",
                required=True,
                min_items=1,
                items=Field("object", fields=dict(item, sql=Field("string", required=True), log=OUTPUT_LOG_FIELD)),
            ),
            runtime=runtime,
        )

    def source_reference_keys_of(self, step, connections):
        """
        A call step's `arguments` and `async_job` are rendered by the source engine too (`{{ partition.* }}`,
        `{{ submit.* }}`), as is a source step's `config` (`{{ partition.* }}`).
        """
        kind = self.classify_step(step, connections)[0]
        if kind == "source_call":
            return self.source_reference_keys | {"arguments", "async_job"}
        if kind == "source":
            return self.source_reference_keys | {"config"}
        return self.source_reference_keys

    def classify_step(self, step, connections):
        if "source" in step:
            return "source", None                          # a source folder's stream, run on the driver
        if "connection" in step:
            conn = connections.get(step["connection"])
            provider = conn.get("provider") if isinstance(conn, dict) else None
            if provider in self.adapters:
                return "call", None                        # a native Spark adapter operation
            return "source_call", None                     # a source connector, run as a driver-side call step
        if isinstance(step.get("sql"), str):
            return "sql", None
        return None, "a step without a connection needs sql"

    def step_policy_keys(self, kind):
        """Execution-policy keys the Spark engine honours per step kind (contract v1.2 §6a)."""
        if kind == "source_call":
            return STEP_POLICY_KEYS                         # call steps throttle and page the connector
        if kind == "source":
            return ("retry", "page_size", "on_error", "timeout")   # the source folder owns its paging and throttle
        if kind == "sql":
            return ("on_error", "timeout")
        return ("retry", "on_error", "timeout")            # native reads/consumes: no client-side paging or throttle

    def output_policy_keys(self):
        return OUTPUT_POLICY_KEYS

    def _applies(self, output, connection):
        return connection.get("provider") not in self.adapters and is_apply_output(output, connection, self.connectors)

    def check_apply(self, output, connection):
        return check_apply(output, connection) if self._applies(output, connection) else None

    def _check_call(self, item, connection, is_step):
        adapter = self.adapters.get(connection["provider"])
        if adapter is None:
            return [("no-adapter", "provider %r has no Spark adapter" % connection["provider"], False)]
        return self._check_defined_call(adapter, item, is_step)

    @staticmethod
    def _check_defined_call(adapter, item, is_step):
        operation = adapter.operation(item.get("service"), item.get("method"))
        if operation is None:
            return [
                (
                    "unknown-method",
                    "provider %s has no %s.%s" % (adapter.provider, item.get("service"), item.get("method")),
                    False,
                )
            ]
        problems = []
        if (is_step and operation.kind not in ("read", "consume")) or (
            not is_step and operation.kind not in ("write", "publish")
        ):
            problems.append(
                ("wrong-role", "the method cannot be used in %s" % ("steps" if is_step else "outputs"), False)
            )
        arguments = item.get("arguments") or {}
        if isinstance(arguments, dict):
            issues = check_arguments(adapter.definition, operation, arguments, adapter.capabilities)
            problems.extend(("bad-argument", _contract_issue(issue), False) for issue in issues)
            replay_safe = arguments.get("mode") in _REPLAY_SAFE_MODES or arguments.get("on_exists") in (
                "skip",
                "overwrite",
            )
            if not is_step and operation.kind == "write" and not operation.idempotent and not replay_safe:
                problems.append(
                    (
                        "not-idempotent",
                        "write may fail or duplicate rows on replay; use a replay-safe mode (%s)%s"
                        % (", ".join(_REPLAY_SAFE_MODES),
                           " or on_exists" if operation.argument("on_exists") is not None else ""),
                        True,
                    )
                )
            if not is_step and operation.argument("target_predicate") is not None and not issues:
                spec = OutputSpec(
                    name=item.get("name") or "output",
                    mode=arguments.get("mode"),
                    target_predicate=arguments.get("target_predicate"),
                )
                problems.extend(("merge-scope", problem.message, True) for problem in warn_merge_scope(spec))
        return problems

    def check_step(self, kind, step, connections):
        problems = []
        if kind == "call":
            problems += self._check_call(step, connections[step["connection"]], True)
        elif kind == "source_call":
            provider = connections[step["connection"]]["provider"]
            if provider not in self.connectors:
                problems.append(("connector-not-installed",
                                 "provider %r has neither a Spark adapter nor an installed connector for calls: %s"
                                 % (provider, install_hint(provider)), False))
            if not any(key in step for key in ("request", "service", "method", "async_job")):
                problems.append(("missing-key", "a call step needs an `operation:` or an `async_job:`", False))
            elif provider in self.connectors:
                problems += call_problems(step, connections[step["connection"]])
            problems += partition_problems(step)
        elif kind == "source":
            if "stream" not in step:
                problems.append(("missing-key", "a source step needs `stream`", False))
            problems += partition_problems(step)
        if step.get("persist") is not None and storage_level_name(step["persist"]) is None:
            problems.append(("bad-storage-level", "invalid Spark StorageLevel %r" % str(step["persist"]).strip(),
                             False))
        return problems

    def check_connection(self, name, connection):
        adapter = self.adapters.get(connection.get("provider"))
        if adapter is None:
            if connection.get("provider") in self.connectors:
                return connection_problems(connection)     # a call step's source connector
            return [("no-adapter", "provider %r has neither a Spark adapter nor an installed connector: %s"
                     % (connection.get("provider"), install_hint(connection.get("provider"))), False)]
        auth = {key: value for key, value in connection.items() if key != "provider"}
        return [
            ("bad-connection", _contract_issue(issue), False) for issue in check_connection(adapter.definition, auth)
        ]

    def write_target(self, connection):
        return WriteTarget("Spark output", (), ())

    def secret_keys(self, connection):
        adapter = self.adapters.get(connection.get("provider"))
        if adapter is not None:
            return adapter.secret_keys
        if connection.get("provider") not in self.connectors:
            return ()
        description = components.describe(connection["provider"])    # a call step's source connector
        return tuple(description["secret_keys"]) if description else ()

    def dependencies_of(self, item):
        return dependencies(item)

    def check_state(self, state, connections):
        if ("connection" in state) == ("sqlite" in state):
            return [("bad-state", "runtime.state needs either connection or sqlite", False)]
        if "connection" in state:
            conn = connections.get(state["connection"])
            if conn is None:
                return [("unknown-connection", "unknown state connection", False)]
            adapter = self.adapters.get(conn["provider"])
            if adapter is None or not adapter.holds_state:
                return [("bad-state", "the state connection cannot hold bookkeeping state", False)]
        return []

    def _check_items(self, job, connections):
        found = []
        for section in ("steps", "outputs"):
            items = job.get(section) if isinstance(job.get(section), list) else ()
            for index, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                path = (section, index)
                if section == "outputs" and item.get("connection") in connections and not self._applies(
                        item, connections[item["connection"]]):
                    found.extend(
                        (code, (*path, "arguments"), message, warning)
                        for code, message, warning in self._check_call(item, connections[item["connection"]], False)
                    )
                sql = item.get("sql")
                if isinstance(sql, str):
                    declared = set((job.get("spec") or {}).get("config") or {})
                    for parameter in sql_parameters(sql):
                        if parameter not in declared | {"batch_id", "batch_ts", "driver_host"}:
                            found.append(
                                (
                                    "unknown-parameter",
                                    (*path, "sql"),
                                    ":%s is not a batch or spec.config name" % parameter,
                                    False,
                                )
                            )
        return found

    def _check_connections(self, connections):
        found = []
        for name, connection in connections.items():
            if not isinstance(connection, dict):
                continue
            adapter = self.adapters.get(connection.get("provider"))
            if adapter:
                found.extend(
                    ("bad-link", ("connections", name), message, False)
                    for message in adapter.check_links(
                        {k: v for k, v in connection.items() if k != "provider"}, connections
                    )
                )
        return found

    def _check_templates(self, job, connections):
        """
        `{{ }}` constants Spark would pass on as written: any in `sql` (Spark SQL takes the :batch_id, :batch_ts and
        :driver_host parameters), and an adapter's own tokens (e.g. a JDBC staging table's {{ uuid }}) anywhere but
        the arguments of that adapter's steps and outputs.
        """
        adapter_only, found = self.builtin_references - _BUILTINS, []
        places = [((section,), job.get(section), frozenset()) for section in ("connections", "runtime")]
        for section in ("steps", "outputs"):
            for index, item in enumerate(job.get(section) if isinstance(job.get(section), list) else ()):
                if isinstance(item, dict):
                    connection = connections.get(item.get("connection"))
                    adapter = self.adapters.get(connection.get("provider")) if isinstance(connection, dict) else None
                    own = frozenset(adapter.template_tokens()) if adapter else frozenset()
                    places.extend(((section, index, key), value, own if key == "arguments" else frozenset())
                                  for key, value in item.items())
        for path, value, own in places:
            for expr in (expr for text in walk_strings(value) for expr in references(text)):
                if path[-1] == "sql" and expr in self.builtin_references:
                    found.append(("unrendered-reference", path, "{{ %s }} is not rendered in sql: use the :batch_id, "
                                  ":batch_ts or :driver_host parameter" % expr, False))
                elif expr in adapter_only and expr not in own:
                    found.append(("unsupported-reference", path, "{{ %s }} is rendered only in the arguments of the "
                                  "adapters that declare it (%s)" % (expr, ", ".join(sorted(
                                      name for name, adapter in self.adapters.items()
                                      if expr in adapter.template_tokens()))), False))
        return found

    def check_engine(self, job):
        connections = job.get("connections") if isinstance(job.get("connections"), dict) else {}
        return (
            self._check_items(job, connections)
            + self._check_connections(connections)
            + check_runtime(job)
            + self._check_templates(job, connections)
            + check_platform(job, self.launchers)
        )
