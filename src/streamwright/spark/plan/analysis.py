"""Spark action and caching analysis on the shared jobs dependency graph."""

import re

from streamwright.core.jobs.calls import partition_dependencies
from streamwright.core.jobs.plan import Node, Plan, item_dependencies, sql_table_reads
from streamwright.core.jobs.plan import build_plan as core_plan
from streamwright.core.jobs.refs import references

__all__ = ["Node", "Plan", "build_plan", "role_of"]

_PARAMETER = re.compile(r"(?<![:\w]):[A-Za-z_][A-Za-z0-9_]*")
_EXPENSIVE = re.compile(r"\b(join|group\s+by|union|distinct|over)\b", re.I)


def _spark_reads(sql):
    return sql_table_reads(_PARAMETER.sub("'value'", sql))


def _dependencies(item):
    return item_dependencies(item, reads_of=_spark_reads) + partition_dependencies(item)


def role_of(job, item, adapters):
    connection = (job.get("connections") or {}).get(item.get("connection"))
    adapter = (adapters or {}).get((connection or {}).get("provider"))
    operation = adapter.operation(item.get("service"), item.get("method")) if adapter else None
    return operation.kind if operation and operation.kind in ("consume", "publish") else None


def _reference_targets(item):
    found = []

    def walk(value):
        if isinstance(value, str):
            found.extend(references(value))
        elif isinstance(value, list):
            for entry in value:
                walk(entry)
        elif isinstance(value, dict):
            for entry in value.values():
                walk(entry)

    walk(item.get("arguments"))
    return found


class SparkPlan:
    """Engine-only insights; nodes/order/dependencies come from the canonical Plan."""

    def __init__(self, plan, actions, warnings):
        self.plan, self.actions, self.warnings = plan, actions, warnings
        self.nodes, self.order = plan.nodes, plan.order

    @property
    def pinned_steps(self):
        return [node.name for node in self.nodes if node.pinned]

    def node(self, name):
        return self.plan.by_name[name]

    def describe(self):
        return "Spark actions per batch (everything else is lazy):\n" + "\n".join(
            "  %-14s %s" % (kind, text) for kind, text in self.actions
        )


def _annotate_nodes(plan):
    by_name = plan.by_name
    for node in plan.nodes:
        targets = [ref.partition(".") for ref in _reference_targets(node.item)]
        node.column_refs = [name for name, _, column in targets if column and name in by_name]
        node.row_refs = [name for name, _, column in targets if not column and name in by_name]
        node.expensive = bool(node.item.get("connection")) or bool(_EXPENSIVE.search(node.item.get("sql") or ""))
        node.persisted = bool(node.item.get("persist") or node.item.get("checkpoint"))
        node.pinned = False
        node.local = False
        node.roots = set()
    for node in plan.nodes:
        for name in node.row_refs:
            if by_name[name].kind == "step":
                by_name[name].pinned = True


def _mark_local_steps(job, plan, adapters):
    by_name = plan.by_name
    for name in plan.order:
        node = by_name[name]
        if node.pinned:
            node.local = True
        elif role_of(job, node.item, adapters) == "consume" or not node.item.get("connection"):
            node.local = all(by_name[dependency].local for dependency in node.uses)


def _assign_evaluation_roots(plan):
    by_name = plan.by_name
    for root in plan.nodes:
        if root.kind != "output" and not root.persisted and not root.pinned:
            continue
        pending, seen = list(root.uses), set()
        while pending:
            name = pending.pop()
            if name in seen:
                continue
            seen.add(name)
            by_name[name].roots.add(root.name)
            if not (by_name[name].persisted or by_name[name].pinned):
                pending.extend(by_name[name].uses)


def _explain_node(job, plan, node, adapters, referenced):
    actions, warnings = [], []
    if node.kind == "output":
        publish = role_of(job, node.item, adapters) == "publish"
        actions.append(
            (
                "event read" if publish else "write",
                (
                    "%s: its rows are collected once (capped) into the outbox; no Spark write" % node.name
                    if publish
                    else "%s: one action (the write); row counts via observe" % node.name
                ),
            )
        )
        return actions, warnings
    collectors = sorted(n.name for n in plan.nodes if node.name in n.column_refs + n.row_refs)
    if collectors:
        label = "pinned: rows collected once and the step rebuilt from them" if node.pinned else "values collected once"
        actions.append(
            ("reference read", "%s: %s, for the {{ }} used by %s" % (node.name, label, ", ".join(collectors)))
        )
    if node.item.get("connection") and (node.item.get("arguments") or {}).get("columns") is not None:
        actions.append(
            ("driver reads", "%s: file listing and one header read per file, on the driver (no Spark job)" % node.name)
        )
    barrier = node.persisted or node.pinned
    if node.name in referenced and not node.local and not barrier:
        warnings.append(
            "%s is collected for a reference and also evaluated by its consumers; without "
            "persist it is computed again" % node.name
        )
    elif not node.local and node.expensive and not barrier and len(node.roots) >= 2:
        warnings.append(
            "%s is evaluated by %d consumers (%s) and recomputed by each; add persist: "
            "memory_and_disk" % (node.name, len(node.roots), ", ".join(sorted(node.roots)))
        )
    if node.persisted and not node.pinned and len(node.roots) < 2 and node.name not in referenced:
        warnings.append(
            "%s is persisted but only %d consumer(s) evaluate it: the cache costs memory for "
            "nothing" % (node.name, len(node.roots))
        )
    return actions, warnings


def build_plan(job, adapters=None):
    plan = core_plan(job, _dependencies)
    _annotate_nodes(plan)
    _mark_local_steps(job, plan, adapters)
    _assign_evaluation_roots(plan)
    actions, warnings = [], []
    referenced = {ref for node in plan.nodes for ref in node.column_refs}
    for node in plan.nodes:
        node_actions, node_warnings = _explain_node(job, plan, node, adapters, referenced)
        actions.extend(node_actions)
        warnings.extend(node_warnings)
    return SparkPlan(plan, actions, warnings)
