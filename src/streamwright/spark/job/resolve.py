"""`{{ ... }}` handling: config and secret substitution, and finding references. PROPOSED."""

import re


__all__ = ["MOUSTACHE", "INPUT_REF", "STEP_REF", "resolve", "references", "walk_strings"]

MOUSTACHE = re.compile(r"\{\{\s*([^{}]*?)\s*\}\}")
INPUT_REF = re.compile(r"^(config|secrets)\.([A-Za-z_][A-Za-z0-9_]*)$")
STEP_REF = re.compile(r"^([a-z][a-z0-9_]*)(?:\.([A-Za-z_][A-Za-z0-9_]*))?$")


def references(text):
    """The {{ ... }} expressions in a string."""
    return [m.group(1) for m in MOUSTACHE.finditer(text)] if isinstance(text, str) else []


def walk_strings(node):
    if isinstance(node, str):
        yield node
    elif isinstance(node, list):
        for item in node:
            for found in walk_strings(item):
                yield found
    elif isinstance(node, dict):
        for item in node.values():
            for found in walk_strings(item):
                yield found


def resolve(job, config=None, secrets=None):
    """Returns (job, problems). Only {{ config.* }} and {{ secrets.* }} are replaced; secrets never reach messages."""
    config, secrets, problems = config or {}, secrets or {}, []

    def sub(text):
        def one(match):
            hit = INPUT_REF.match(match.group(1))
            if not hit:
                return match.group(0)
            source, name = hit.groups()
            values = config if source == "config" else secrets
            if name not in values or values[name] is None:
                problems.append("%s.%s has no value" % (source, name))
                return match.group(0)
            return str(values[name])
        return MOUSTACHE.sub(one, text)

    def walk(node):
        if isinstance(node, str):
            return sub(node)
        if isinstance(node, list):
            return [walk(item) for item in node]
        if isinstance(node, dict):
            return dict((key, walk(value)) for key, value in node.items())
        return node

    return walk(job), sorted(set(problems))
