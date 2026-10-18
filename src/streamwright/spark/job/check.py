"""
Checking a `kind: SparkJob` without Spark, brokers or credentials. PROPOSED.

check_job(job, adapters, launchers=None, loader=None) -> (problems, warnings)

The job file itself (sections, names, references, runtime keys) is checked here. Everything about a provider is asked
of its adapter, which asks the existing connector: the connection's keys (`check_auth`), the call's arguments
(`check_request` for the ones the connector knows), the methods the provider offers. This module never names a provider.
"""

import re

from streamwright.spark.contracts import LAUNCHER_OWNED_CONF, AdapterError, Kind, Role
from streamwright.spark.job.resolve import INPUT_REF, STEP_REF, references, walk_strings


__all__ = ["check_job", "RESERVED_NAMES", "NAME"]

NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
RESERVED_NAMES = frozenset(["input", "batch", "config", "secrets", "_commits", "_file_errors"])
_TOP = ("kind", "version", "name", "description", "spec", "connections", "steps", "outputs", "runtime")
_ITEM_KEYS = ("name", "connection", "service", "method", "arguments", "sql", "persist", "checkpoint")
_PERSIST_SHORTHANDS = {"memory": "MEMORY_ONLY", "memory_and_disk": "MEMORY_AND_DISK", "disk": "DISK_ONLY"}
_PYSPARK_STORAGE_LEVELS = frozenset([
    "MEMORY_ONLY", "MEMORY_ONLY_2", "MEMORY_ONLY_SER", "MEMORY_ONLY_SER_2",
    "MEMORY_AND_DISK", "MEMORY_AND_DISK_2", "MEMORY_AND_DISK_SER", "MEMORY_AND_DISK_SER_2", "MEMORY_AND_DISK_DESER",
    "DISK_ONLY", "DISK_ONLY_2", "DISK_ONLY_3",
    "OFF_HEAP", "NONE"
])
_SECRET_REF = re.compile(r"^\{\{\s*secrets\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}$")
_NAMED_PARAM = re.compile(r"(?<![:\w]):([A-Za-z_][A-Za-z0-9_]*)")
_QUOTED = re.compile(r"'(?:[^']|'')*'")


class _Context(object):
    def __init__(self, adapters, launchers, loader):
        self.adapters, self.launchers, self.loader = adapters, launchers, loader
        self.problems, self.warnings = [], []
        self.connectors, self.connections = {}, {}
        self.config_names, self.secret_names = set(), set()

    def add(self, text):
        self.problems.append(text)

    def connector_of(self, adapter):
        if adapter.provider not in self.connectors:
            try:
                self.connectors[adapter.provider] = adapter.load_connector(self.loader)
            except AdapterError as exc:
                self.add(str(exc))
                self.connectors[adapter.provider] = False
        return self.connectors[adapter.provider] or None


def check_job(job, adapters, launchers=None, loader=None):
    ctx = _Context(adapters, launchers, loader)
    if not isinstance(job, dict):
        return ["the job must be a mapping"], []
    for key in job:
        if key not in _TOP:
            ctx.add("unknown top-level key %r (sections: %s)" % (key, ", ".join(_TOP)))
    if job.get("kind") != "SparkJob":
        ctx.add("kind must be SparkJob")
    if not isinstance(job.get("name"), str) or not NAME.match(job.get("name") or ""):
        ctx.add("name must be lower case letters, digits and _")
    ctx.config_names = _declared(job, "config", ctx)
    ctx.secret_names = _declared(job, "secrets", ctx)
    _check_connections(job.get("connections"), ctx)
    steps, outputs = job.get("steps") or [], job.get("outputs") or []
    if not isinstance(steps, list) or not isinstance(outputs, list):
        return ctx.problems + ["steps and outputs must be lists"], ctx.warnings
    if not outputs:
        ctx.add("outputs: at least one output is required")
    seen, known, consumers = set(), set(), 0
    for index, step in enumerate(steps):
        consumers += _check_item(step, "steps[%d]" % index, True, ctx, seen, known)
        if isinstance(step, dict) and isinstance(step.get("name"), str):
            known.add(step["name"])
    for index, out in enumerate(outputs):
        _check_item(out, "outputs[%d]" % index, False, ctx, seen, known)
    if consumers > 1:
        ctx.add("steps: at most one consume step per job")
    _check_runtime(job.get("runtime"), outputs, ctx)
    return ctx.problems, ctx.warnings


def _declared(job, section, ctx):
    spec = job.get("spec") or {}
    if not isinstance(spec, dict):
        ctx.add("spec must be a mapping")
        return set()
    items = spec.get(section) or {}
    if not isinstance(items, dict):
        ctx.add("spec.%s must be a mapping" % section)
        return set()
    for name, definition in items.items():
        if not isinstance(name, str) or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
            ctx.add("spec.%s: invalid name %r" % (section, name))
        if definition is not None and not isinstance(definition, dict):
            ctx.add("spec.%s.%s must be a mapping" % (section, name))
    return set(items)


def _check_connections(connections, ctx):
    if not isinstance(connections, dict) or not connections:
        ctx.add("connections: at least one connection is required")
        return
    for name, conn in connections.items():
        where = "connections.%s" % name
        if not isinstance(name, str) or not NAME.match(name):
            ctx.add("%s: invalid connection name" % where)
        if not isinstance(conn, dict) or "provider" not in conn:
            ctx.add("%s: needs a provider" % where)
            continue
        adapter = ctx.adapters.get(conn["provider"])
        if adapter is None:
            ctx.add("%s: no Spark support for provider %r (available: %s). Install its connector with `streamwright "
                    "connectors install %s`, and a Spark adapter must exist for it" % (
                        where, conn["provider"], ", ".join(sorted(ctx.adapters)) or "none", conn["provider"]))
            continue
        ctx.connections[name] = (conn, adapter)
        auth = dict((k, v) for k, v in conn.items() if k != "provider")
        for key, value in auth.items():
            for ref in walk_strings(value):
                for expr in references(ref):
                    hit = INPUT_REF.match(expr)
                    if not hit:
                        ctx.add("%s.%s: only config.* and secrets.* references are allowed here" % (where, key))
                    elif hit.group(1) == "secrets" and hit.group(2) not in ctx.secret_names:
                        ctx.add("%s.%s: secret %r is not declared in spec.secrets" % (where, key, hit.group(2)))
                    elif hit.group(1) == "config" and hit.group(2) not in ctx.config_names:
                        ctx.add("%s.%s: config %r is not declared in spec.config" % (where, key, hit.group(2)))
        for key in adapter.secret_keys:
            if auth.get(key) is not None and not (isinstance(auth[key], str) and _SECRET_REF.match(auth[key].strip())):
                ctx.add("%s.%s must be a {{ secrets.NAME }} reference, not a literal" % (where, key))
        connector = ctx.connector_of(adapter)
        if connector is not None or adapter.connector is None:
            for problem in adapter.check_connection(auth, connector):
                ctx.add("%s: %s" % (where, problem))
    for name, (conn, adapter) in ctx.connections.items():
        for problem in adapter.check_links(dict((k, v) for k, v in conn.items() if k != "provider"),
                                           dict((n, c) for n, (c, _) in ctx.connections.items())):
            ctx.add("connections.%s: %s" % (name, problem))


def _check_item(item, where, is_step, ctx, seen, known):
    if not isinstance(item, dict):
        ctx.add("%s must be a mapping" % where)
        return 0
    for key in item:
        if key not in _ITEM_KEYS:
            ctx.add("%s: unknown key %r" % (where, key))
    name = item.get("name")
    if not isinstance(name, str) or not NAME.match(name) or name in RESERVED_NAMES:
        ctx.add("%s: name must be lower case letters, digits and _ and not reserved (%s)" % (
            where, ", ".join(sorted(RESERVED_NAMES))))
    elif name in seen:
        ctx.add("%s: duplicate name %r" % (where, name))
    else:
        seen.add(name)
    sql, consumers = item.get("sql"), 0
    if sql is not None and not isinstance(sql, str):
        ctx.add("%s.sql must be text" % where)
        sql = None
    if "connection" in item:
        entry = ctx.connections.get(item["connection"])
        if entry is None:
            if item["connection"] not in (ctx.connections or {}):
                ctx.add("%s: unknown connection %r" % (where, item["connection"]))
        else:
            consumers = _check_call(item, where, is_step, entry[1], ctx)
    else:
        for key in ("service", "method", "arguments"):
            if key in item:
                ctx.add("%s: %s needs a connection" % (where, key))
        if sql is None:
            ctx.add("%s: a step without a connection needs sql" % where)
    if not is_step and sql is None:
        ctx.add("%s: an output needs sql" % where)
    _check_refs(where, item, known, ctx)
    if item.get("persist") is not None:
        raw = str(item["persist"]).strip()
        canonical = _PERSIST_SHORTHANDS.get(raw.lower(), raw.upper())
        if canonical not in _PYSPARK_STORAGE_LEVELS:
            ctx.add("%s.persist: %r is not a valid Spark StorageLevel (expected e.g. MEMORY_AND_DISK, MEMORY_AND_DISK_SER, MEMORY_ONLY, DISK_ONLY, OFF_HEAP)" % (where, item["persist"]))
    return consumers


def _check_call(item, where, is_step, adapter, ctx):
    method = adapter.method(item.get("service"), item.get("method"))
    if method is None:
        ctx.add("%s: %s has no %s.%s (has: %s)" % (where, adapter.provider, item.get("service"), item.get("method"),
                                                  adapter.describe_methods()))
        return 0
    if is_step and method.kind == Kind.WRITE:
        ctx.add("%s: %s.%s writes; use it under outputs" % (where, method.service, method.name))
    if not is_step and method.kind == Kind.READ:
        ctx.add("%s: %s.%s reads; use it under steps" % (where, method.service, method.name))
    if not is_step and method.role == Role.CONSUME:
        ctx.add("%s: consume belongs under steps" % where)
    if is_step and method.role == Role.PUBLISH:
        ctx.add("%s: publish belongs under outputs" % where)
    arguments = item.get("arguments")
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, dict):
        ctx.add("%s.arguments must be a mapping" % where)
        return 0
    for problem in adapter.check_call(method, arguments, ctx.connector_of(adapter)):
        ctx.add("%s.arguments: %s" % (where, problem))
    for warning in adapter.warn_call(method, arguments):
        ctx.warnings.append("%s: %s" % (where, warning))
    return 1 if (is_step and method.role == Role.CONSUME) else 0


def _check_refs(where, item, known, ctx):
    for text in walk_strings(item.get("arguments")):
        for expr in references(text):
            if expr in ("batch.id", "batch.ts"):
                continue
            declared = INPUT_REF.match(expr)
            if declared:
                if declared.group(1) == "config" and declared.group(2) not in ctx.config_names:
                    ctx.add("%s: config %r is not declared in spec.config" % (where, declared.group(2)))
                continue
            hit = STEP_REF.match(expr)
            if not hit or hit.group(1) not in known:
                ctx.add("%s: %r refers to a step that is not defined before it" % (where, "{{ %s }}" % expr))
    sql = item.get("sql")
    if isinstance(sql, str):
        for param in _NAMED_PARAM.findall(_QUOTED.sub("''", sql)):
            if param not in ("batch_id", "batch_ts") and param not in ctx.config_names:
                ctx.add("%s.sql: :%s is not batch_id, batch_ts or a spec.config name" % (where, param))


def _check_runtime(runtime, outputs, ctx):
    if not isinstance(runtime, dict):
        ctx.add("runtime: required (state, spark, platform)")
        return
    state = runtime.get("state")
    if not isinstance(state, dict):
        ctx.add("runtime.state is required: {connection, schema} or {sqlite: PATH}")
    elif "sqlite" in state:
        if not isinstance(state["sqlite"], str) or not state["sqlite"]:
            ctx.add("runtime.state.sqlite must be a file path")
    else:
        entry = ctx.connections.get(state.get("connection"))
        if entry is None:
            ctx.add("runtime.state.connection must name a connection")
        else:
            if not entry[1].holds_state:
                ctx.add("runtime.state.connection: %s cannot hold bookkeeping state" % entry[1].provider)
            if not re.match(r"^[a-z][a-z0-9_]*$", str(state.get("schema", ""))):
                ctx.add("runtime.state.schema must be a plain schema name")
            for out in outputs:
                if isinstance(out, dict) and out.get("connection") == state["connection"]:
                    ctx.add("outputs.%s writes through the bookkeeping connection; use a separate one" % out.get("name"))
    secrets = runtime.get("secrets")
    if secrets is not None:
        if not isinstance(secrets, dict) or secrets.get("source") not in ("env", "aws_secretsmanager"):
            ctx.add("runtime.secrets.source must be env or aws_secretsmanager")
        elif secrets["source"] == "aws_secretsmanager" and not isinstance(secrets.get("id"), str):
            ctx.add("runtime.secrets.id is required for aws_secretsmanager")
    conf = ((runtime.get("spark") or {}).get("conf")) or {}
    if not isinstance(conf, dict):
        ctx.add("runtime.spark.conf must be a mapping")
        conf = {}
    for key, value in conf.items():
        if key in LAUNCHER_OWNED_CONF:
            ctx.add("runtime.spark.conf.%s is owned by the launcher and the lock" % key)
        elif not str(key).startswith("spark."):
            ctx.add("runtime.spark.conf.%s must start with spark." % key)
        elif isinstance(value, (dict, list)):
            ctx.add("runtime.spark.conf.%s must be a scalar" % key)
    platform = runtime.get("platform")
    if not isinstance(platform, dict) or not platform.get("type"):
        ctx.add("runtime.platform.type is required")
    elif ctx.launchers is not None:
        launcher = ctx.launchers.get(platform["type"])
        if launcher is None:
            ctx.add("runtime.platform.type %r has no launcher (available: %s)" % (
                platform["type"], ", ".join(sorted(ctx.launchers)) or "none"))
        else:
            for problem in launcher.check_platform(platform):
                ctx.add("runtime.platform: %s" % problem)
    if platform and platform.get("type") not in (None, "local") and not (runtime.get("artifacts") or {}).get("lock"):
        ctx.warnings.append("runtime.artifacts.lock is not set: package and jar versions are not pinned")
    for key, value in (runtime.get("limits") or {}).items():
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            ctx.add("runtime.limits.%s must be a positive integer" % key)
