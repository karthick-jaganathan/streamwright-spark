"""Spark-only rule tables and adapter hooks for the shared jobs checker."""

import re
from types import MappingProxyType

from streamwright.core.jobs import JobRules
from streamwright.core.jobs.calls import partition_problems
from streamwright.core.jobs.check.rules import NAME_PATTERN, input_field
from streamwright.core.jobs.fields import Field
from streamwright.core.jobs.policy import OUTPUT_POLICY_KEYS, STEP_POLICY_KEYS
from streamwright.core.jobs.write_rules import OutputSpec, WriteTarget, warn_merge_scope
from streamwright.core.runtime import components
from streamwright.core.sdk.contract import check_arguments, check_connection
from streamwright.spark.job.runtime_rules import check_platform, check_runtime
from streamwright.spark.plan.analysis import _dependencies

__all__ = ["SparkRules"]

_TEXT = Field("string")
# the execution-policy vocabulary (contract v1.2 §6a), accepted structurally on steps and outputs
_POLICY_FIELDS = MappingProxyType({
    "retry": Field("object"), "rate_limit": Field("object"), "paginator": Field("object"),
    "page_size": Field("integer", minimum=1), "on_error": _TEXT,
    "timeout": Field(("number", "string"), description="per-call timeout, seconds or a duration like `30s`"),
})
# a source-connector call step (contract v1.2 §6a engine reach): service/method or an HTTP request, run on the driver
_CALL_FIELDS = MappingProxyType({
    "service": _TEXT, "method": _TEXT, "arguments": Field("object"),
    "request": Field("object", fields={"path": _TEXT, "method": _TEXT, "params": Field("object"),
                                       "headers": Field("object"), "json": Field(), "records": Field("object")}),
    # one call per distinct value of an earlier step's column, read as {{ partition.<name> }} (core jobs.calls)
    "partitions": Field("array", items=Field("object", fields={"from": Field("string", required=True),
                                                               "field": Field("string", required=True),
                                                               "name": _TEXT})),
})
_STORAGE_LEVELS = frozenset({
    "MEMORY_ONLY", "MEMORY_ONLY_2", "MEMORY_ONLY_SER", "MEMORY_ONLY_SER_2", "MEMORY_AND_DISK", "MEMORY_AND_DISK_2",
    "MEMORY_AND_DISK_SER", "MEMORY_AND_DISK_SER_2", "MEMORY_AND_DISK_DESER", "DISK_ONLY", "DISK_ONLY_2",
    "DISK_ONLY_3", "OFF_HEAP", "NONE",
})
_PERSIST = MappingProxyType({"memory": "MEMORY_ONLY", "memory_and_disk": "MEMORY_AND_DISK", "disk": "DISK_ONLY"})
_PARAMETER = re.compile(r"(?<![:\w]):([A-Za-z_][A-Za-z0-9_]*)")
_QUOTED = re.compile(r"'(?:[^']|'')*'")
_REPLAY_SAFE_MODES = ("overwrite", "merge", "delete")
_BUILTINS = frozenset((
    "batch.id", "batch.ts", "batch.id_safe", "batch_id_safe", "batch.seq", "batch_seq", "driver.host", "driver_host",
))


def _object(**fields):
    return Field("object", fields=fields)


def _contract_issue(issue):
    return f"{issue.path}: {issue.message}" if issue.path else issue.message


class SparkRules(JobRules):
    kind = "SparkJob"
    builtin_references = _BUILTINS
    step_reference_keys = frozenset(("arguments",))
    source_reference_keys = frozenset(("request", "paginator"))  # rendered by the source engine on a call step
    step_kinds = MappingProxyType(
        {
            "sql": ("name", "sql", "persist", "checkpoint", "view"),
            "call": ("name", "connection", "service", "method", "arguments", "sql", "persist", "checkpoint", "view"),
            "source_call": ("name", "connection", "service", "method", "arguments", "request", "partitions", "sql",
                            "persist", "checkpoint", "view"),
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
            "service": _TEXT,
            "method": _TEXT,
            "arguments": Field("object"),
            "sql": _TEXT,
            "persist": _TEXT,
            "checkpoint": Field("boolean"),
            "view": Field("string", enum=("local", "global")),
            "request": _CALL_FIELDS["request"],
            **_POLICY_FIELDS,
        }
        runtime = _object(
            state=_object(connection=_TEXT, schema=_TEXT, sqlite=_TEXT, ttl_seconds=Field("integer", minimum=1)),
            spark=_object(app_name=_TEXT, version=_TEXT, conf=Field("object")),
            platform=Field("object"),
            limits=Field("object"),
            env=Field("object"),
            secrets=Field("object"),
            artifacts=Field("object"),
        )
        variants = {}
        for provider, adapter in self.adapters.items():
            variants[provider] = {field.name: self._definition_field(field) for field in adapter.definition.connection}
        connection = Field(
            "object",
            fields={"provider": Field("string", required=True)},
            open=True,
            discriminator="provider",
            variants=variants,
        )
        return _object(
            kind=Field("string", required=True, enum=("SparkJob",)),
            version=Field(("integer", "string")),
            name=Field("string", required=True, pattern=NAME_PATTERN),
            description=_TEXT,
            spec=_object(config=Field("object", values=input_field()), secrets=Field("object", values=input_field())),
            connections=Field("object", required=True, min_items=1, values=connection),
            steps=Field("array", items=Field("object", fields=dict(item, partitions=_CALL_FIELDS["partitions"]))),
            outputs=Field(
                "array",
                required=True,
                min_items=1,
                items=Field("object", fields=dict(item, sql=Field("string", required=True))),
            ),
            runtime=runtime,
        )

    def source_reference_keys_of(self, step, connections):
        """A call step's `arguments` are rendered by the source engine too (`{{ partition.* }}`, windows)."""
        if self.classify_step(step, connections)[0] == "source_call":
            return self.source_reference_keys | {"arguments"}
        return self.source_reference_keys

    def classify_step(self, step, connections):
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
        if kind == "sql":
            return ("on_error", "timeout")
        return ("retry", "on_error", "timeout")            # native reads/consumes: no client-side paging or throttle

    def output_policy_keys(self):
        return OUTPUT_POLICY_KEYS

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
                        "write may fail or duplicate rows on replay; use a replay-safe "
                        "mode (%s) or on_exists" % ", ".join(_REPLAY_SAFE_MODES),
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
                                 "provider %r has neither a Spark adapter nor an installed connector for calls"
                                 % provider, False))
            if "request" in step and ("service" in step or "method" in step):
                problems.append(("bad-value", "a call is `request` (HTTP) or `service`/`method` (SDK), not both",
                                 False))
            elif "request" not in step and "service" not in step and "method" not in step:
                problems.append(("missing-key", "a call step needs `service`/`method` or a `request`", False))
            problems += partition_problems(step)
        if step.get("persist") is not None:
            value = str(step["persist"]).strip()
            if _PERSIST.get(value.lower(), value.upper()) not in _STORAGE_LEVELS:
                problems.append(("bad-storage-level", "invalid Spark StorageLevel %r" % value, False))
        return problems

    def check_connection(self, name, connection):
        adapter = self.adapters.get(connection.get("provider"))
        if adapter is None:
            if connection.get("provider") in self.connectors:
                return []                                  # a call-step source connector; the source engine checks it
            return [("no-adapter", "provider %r has neither a Spark adapter nor an installed connector "
                     "(pip install streamwright-%s)" % (connection.get("provider"), connection.get("provider")),
                     False)]
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
        return _dependencies(item)

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
                if section == "outputs" and item.get("connection") in connections:
                    found.extend(
                        (code, (*path, "arguments"), message, warning)
                        for code, message, warning in self._check_call(item, connections[item["connection"]], False)
                    )
                sql = item.get("sql")
                if isinstance(sql, str):
                    declared = set((job.get("spec") or {}).get("config") or {})
                    for parameter in _PARAMETER.findall(_QUOTED.sub("''", sql)):
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

    def check_engine(self, job):
        connections = job.get("connections") if isinstance(job.get("connections"), dict) else {}
        return (
            self._check_items(job, connections)
            + self._check_connections(connections)
            + check_runtime(job)
            + check_platform(job, self.launchers)
        )
