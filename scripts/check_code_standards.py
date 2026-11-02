#!/usr/bin/env python3
# vendored from streamwright/scripts/check_code_standards.py — keep identical (streamwright's CI compares the copies
# in streamwright-spark and streamwright-mcp with it)
"""
The code standards ruff cannot check (StreamWright docs/design/connector-contract.md section 7), dependency-free
(stdlib ast):

  mutable-module-state   a module-level name bound to a dict/list/set literal or comprehension, or to a dict(),
                         list(), set(), defaultdict(), OrderedDict(), Counter() or deque() call (wrap constants in
                         MappingProxyType, frozenset or tuple)
  global-statement       a `global` statement (unless its line says `# noqa: PLW0603`, as ruff takes it)
  function-import        an import inside a function (imports go at the module top), unless its line says
                         `# noqa: PLC0415` with the reason, as ruff takes it
  module-too-long        a module of more than MAX_MODULE_LINES physical lines (split it by responsibility); tests
                         (tests/ folders, test_*.py, conftest.py) are exempt

Usage: check_code_standards.py [--strict] [PATH ...]
PATHs are relative to the working directory; by default the repository's Python packages (those of DEFAULT_PATHS
that exist next to this script's folder). Report-only unless --strict, which exits 1 when anything is found.
"""

import argparse
import ast
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

ROOT = Path(__file__).resolve().parents[1]
# streamwright: core and connectors; streamwright-spark and streamwright-mcp: src and tests
DEFAULT_PATHS = ("core/src", "core/tests", "connectors", "scripts", "src", "tests")
SKIPPED_PARTS = frozenset({"build", "_site", "__pycache__", ".venv", "node_modules"})
MUTABLE_CALLS = frozenset({"dict", "list", "set", "defaultdict", "OrderedDict", "Counter", "deque"})
MUTABLE_NODES = (ast.Dict, ast.List, ast.Set, ast.DictComp, ast.ListComp, ast.SetComp)
SPECIAL_NAMES = frozenset({"__all__"})  # read by tools as a list by convention
RULES = ("mutable-module-state", "global-statement", "function-import", "module-too-long")
MAX_MODULE_LINES = 400
NOQA = re.compile(r"#\s*noqa:\s*(?P<codes>[A-Z]+[0-9]+(?:\s*,\s*[A-Z]+[0-9]+)*)")


class Finding(NamedTuple):
    path: str
    line: int
    rule: str
    message: str

    def __str__(self) -> str:
        return "%s:%d: %s %s" % (self.path, self.line, self.rule, self.message)


def _call_name(node: ast.AST) -> str | None:
    if not isinstance(node, ast.Call):
        return None
    function = node.func
    if isinstance(function, ast.Name):
        return function.id
    return function.attr if isinstance(function, ast.Attribute) else None


def _is_mutable(value: ast.AST | None) -> bool:
    return isinstance(value, MUTABLE_NODES) or _call_name(value) in MUTABLE_CALLS


def _targets(node: ast.stmt) -> list[str]:
    if isinstance(node, ast.Assign):
        found = node.targets
    elif isinstance(node, ast.AnnAssign):
        found = [node.target]
    else:
        return []
    return [target.id for target in found if isinstance(target, ast.Name)]


def _module_state(path: str, tree: ast.Module) -> Iterator[Finding]:
    for node in tree.body:
        names = [name for name in _targets(node) if name not in SPECIAL_NAMES]
        if names and _is_mutable(getattr(node, "value", None)):
            message = "%s is a mutable module-level container" % ", ".join(names)
            yield Finding(path, node.lineno, "mutable-module-state", message)


class _FunctionLevel(ast.NodeVisitor):
    """`global` statements and imports inside functions, named by their innermost function."""

    def __init__(self, path: str, lines: list[str]):
        self.path = path
        self.lines = lines
        self.functions: list[str] = []
        self.findings: list[Finding] = []

    def _excused(self, node: ast.stmt, code: str) -> bool:
        """Whether the statement's line has a `# noqa: <code>` (with a reason, as ruff takes it)."""
        match = NOQA.search(self.lines[node.lineno - 1])
        return match is not None and code in re.split(r"\s*,\s*", match.group("codes"))

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.functions.append(node.name)
        self.generic_visit(node)
        self.functions.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.visit_FunctionDef(node)

    def visit_Global(self, node: ast.Global) -> None:
        if not self._excused(node, "PLW0603"):
            message = "global %s" % ", ".join(node.names)
            self.findings.append(Finding(self.path, node.lineno, "global-statement", message))

    def visit_Import(self, node: ast.Import | ast.ImportFrom) -> None:
        if self.functions and not self._excused(node, "PLC0415"):
            message = "import inside %s()" % self.functions[-1]
            self.findings.append(Finding(self.path, node.lineno, "function-import", message))

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        self.visit_Import(node)


def _is_test(path: Path) -> bool:
    return "tests" in path.parts or path.name.startswith("test_") or path.name == "conftest.py"


def _module_length(path: Path, shown: str, lines: list[str]) -> Iterator[Finding]:
    if len(lines) > MAX_MODULE_LINES and not _is_test(path):
        yield Finding(shown, 1, "module-too-long", "%d lines (at most %d: split it by responsibility)" % (
            len(lines), MAX_MODULE_LINES))


def check_file(path: Path) -> list[Finding]:
    shown = str(path.relative_to(Path.cwd())) if path.is_relative_to(Path.cwd()) else str(path)
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    tree = ast.parse(text, filename=shown)
    visitor = _FunctionLevel(shown, lines)
    visitor.visit(tree)
    findings = [*_module_length(path, shown, lines), *_module_state(shown, tree), *visitor.findings]
    return sorted(findings, key=lambda finding: finding.line)


def python_files(paths: list[Path]) -> list[Path]:
    files: set[Path] = set()
    for path in paths:
        if not path.exists():
            raise SystemExit("check_code_standards: %s does not exist" % path)
        candidates = [path] if path.is_file() else path.rglob("*.py")
        files.update(item for item in candidates if not SKIPPED_PARTS & set(item.parts))
    return sorted(files)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("paths", nargs="*", type=Path)
    parser.add_argument("--strict", action="store_true", help="exit 1 when anything is found")
    arguments = parser.parse_args(argv)
    paths = [path.resolve() for path in arguments.paths] or [
        ROOT / path for path in DEFAULT_PATHS if (ROOT / path).exists()]
    findings = [finding for path in python_files(paths) for finding in check_file(path)]
    for finding in findings:
        print(finding)
    counts = {rule: sum(1 for finding in findings if finding.rule == rule) for rule in RULES}
    print("code standards: %d finding(s) (%s)" % (len(findings), ", ".join("%s %d" % item for item in counts.items())))
    return 1 if findings and arguments.strict else 0


if __name__ == "__main__":
    sys.exit(main())
