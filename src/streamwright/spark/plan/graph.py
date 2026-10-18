"""
The execution plan of a job, computed without Spark: dependencies, where Spark actions happen, and recomputation
hazards. `sw spark validate --analyze` prints this, and the executor uses `pinned_steps` to materialize small inputs.

Rules the plan makes visible (PROPOSED):
  * steps only build lazy DataFrames/views; they never run an action
  * each output performs exactly one action: its write (counts come from observe, not count())
  * the only other Spark actions are the reference reads: a step whose column or rows are used in `{{ ... }}`
    (collected once, capped by runtime.limits.reference_values, pinned in the manifest)
  * a step used whole (`{{ step }}`) is PINNED: its rows are collected once and the step is rebuilt from them, so a
    mapping table is read once per batch and cannot change between two outputs
  * a step is recomputed by every evaluation root that reaches it. Roots are the outputs and any persisted or pinned
    step; a persisted/pinned step is a barrier (evaluated once, its upstream is not reached through it). The plan
    warns when an expensive step has two or more roots and no barrier.
Dependencies are found by scanning SQL for step names (quoted text and comments removed), which can over-report
when a column shares a step's name but never under-reports a table reference.
"""

import re

from streamwright.spark.contracts import Role
from streamwright.spark.job.resolve import references


__all__ = ["Node", "Plan", "build_plan", "role_of"]

_QUOTED = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")
_COMMENTS = re.compile(r"--[^\n]*|/\*.*?\*/", re.S)
_EXPENSIVE = re.compile(r"\b(join|group\s+by|union|distinct|over)\b", re.I)


class Node(object):
    def __init__(self, name, kind, item):
        self.name, self.kind, self.item = name, kind, item            # kind: step | output
        self.uses = []                                                 # earlier step names this node reads
        self.column_refs = []                                          # steps whose column values it collects
        self.row_refs = []                                             # steps whose rows it collects (whole step)
        self.local = False                                             # cheap and deterministic to recompute
        self.expensive = False                                         # reads data or joins/aggregates
        self.pinned = False                                            # used whole by a {{ step }} reference
        self.persisted = False
        self.roots = set()                                             # evaluation roots that reach it
        self.outputs = set()                                           # outputs depending on it (transitively)


class Plan(object):
    def __init__(self, nodes, actions, warnings):
        self.nodes, self.actions, self.warnings = nodes, actions, warnings

    @property
    def pinned_steps(self):
        return [n.name for n in self.nodes if n.pinned]

    def node(self, name):
        for node in self.nodes:
            if node.name == name:
                return node
        raise KeyError(name)

    def describe(self):
        lines = ["Spark actions per batch (everything else is lazy):"]
        lines += ["  %-14s %s" % (kind, text) for kind, text in self.actions]
        return "\n".join(lines)


def _scan(sql):
    return _COMMENTS.sub(" ", _QUOTED.sub("''", sql or ""))


def role_of(job, item, adapters):
    """Role.CONSUME / Role.PUBLISH from the adapter's method table (None when unknown): no service names here."""
    conn = (job.get("connections") or {}).get(item.get("connection"))
    adapter = (adapters or {}).get((conn or {}).get("provider"))
    method = adapter.method(item.get("service"), item.get("method")) if adapter else None
    return method.role if method else None


def _strings_of(node):
    if isinstance(node, str):
        yield node
    elif isinstance(node, list):
        for value in node:
            for found in _strings_of(value):
                yield found
    elif isinstance(node, dict):
        for value in node.values():
            for found in _strings_of(value):
                yield found


def build_plan(job, adapters=None):
    nodes, by_name = [], {}
    for kind, items in (("step", job.get("steps") or []), ("output", job.get("outputs") or [])):
        for item in items:
            node = Node(item["name"], kind, item)
            nodes.append(node)
            by_name[node.name] = node
    step_names = [n.name for n in nodes if n.kind == "step"]
    for node in nodes:
        item = node.item
        text = _scan(item.get("sql"))
        for other in step_names:
            if other != node.name and re.search(r"(?<![\w.])%s(?![\w(])" % re.escape(other), text):
                node.uses.append(other)
        for value in _strings_of(item.get("arguments")):
            for expr in references(value):
                head, _, column = expr.partition(".")
                if head in by_name and by_name[head].kind == "step":
                    if head not in node.uses:
                        node.uses.append(head)
                    (node.column_refs if column else node.row_refs).append(head)
        node.expensive = bool(item.get("connection")) or bool(_EXPENSIVE.search(text))
        node.persisted = bool(item.get("persist")) or bool(item.get("checkpoint"))
    for node in nodes:
        for head in node.row_refs:
            by_name[head].pinned = True
    for node in nodes:                                   # steps in order: uses are always earlier
        if node.kind != "step":
            continue
        if node.pinned:
            node.local = True
        elif role_of(job, node.item, adapters) == Role.CONSUME or not node.item.get("connection"):
            node.local = all(by_name[u].local for u in node.uses)
    roots = [n for n in nodes if n.kind == "output" or n.persisted or n.pinned]
    for root in roots:
        stack, seen = list(root.uses), set()
        while stack:
            name = stack.pop()
            if name in seen:
                continue
            seen.add(name)
            by_name[name].roots.add(root.name)
            if by_name[name].persisted or by_name[name].pinned:
                continue                                  # a barrier: evaluated once; its upstream is not reached
            stack.extend(by_name[name].uses)
    for node in nodes:
        if node.kind == "output":
            stack, seen = list(node.uses), set()
            while stack:
                name = stack.pop()
                if name not in seen:
                    seen.add(name)
                    by_name[name].outputs.add(node.name)
                    stack.extend(by_name[name].uses)
    actions, warnings = [], []
    for node in nodes:
        if node.kind == "output":
            if role_of(job, node.item, adapters) == Role.PUBLISH:
                actions.append(("event read", "%s: its rows are collected once (capped) into the outbox; no Spark "
                                              "write" % node.name))
            else:
                actions.append(("write", "%s: one action (the write); row counts via observe" % node.name))
            continue
        collectors = sorted(set(n.name for n in nodes if node.name in n.column_refs or node.name in n.row_refs))
        if collectors:
            how = "pinned: rows collected once and the step rebuilt from them" if node.pinned else "values collected once"
            actions.append(("reference read", "%s: %s, for the {{ }} used by %s" % (node.name, how, ", ".join(collectors))))
        if node.item.get("connection") and (node.item.get("arguments") or {}).get("columns") is not None:
            actions.append(("driver reads", "%s: file listing and one header read per file, on the driver (no Spark "
                                            "job)" % node.name))
        barrier = node.persisted or node.pinned
        if node.name in [h for n in nodes for h in n.column_refs] and not node.local and not barrier:
            warnings.append("%s is collected for a reference and also evaluated by its consumers; without persist it "
                            "is computed again" % node.name)
        elif not node.local and node.expensive and not barrier and len(node.roots) >= 2:
            warnings.append("%s is evaluated by %d consumers (%s) and recomputed by each; add persist: "
                            "memory_and_disk" % (node.name, len(node.roots), ", ".join(sorted(node.roots))))
        if node.persisted and not node.pinned and len(node.roots) < 2 and node.name not in [
                h for n in nodes for h in n.column_refs]:
            warnings.append("%s is persisted but only %d consumer(s) evaluate it: the cache costs memory for nothing"
                            % (node.name, len(node.roots)))
    return Plan(nodes, actions, warnings)
