"""Architectural dependency tests for runtime and tooling ownership."""

from __future__ import annotations

import ast
from collections.abc import Iterable
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SEMANTIC_SOURCE = REPOSITORY_ROOT / "packages" / "extended-opentelemetry-semconv" / "src" / "extended_otel_semconv"
CODEGEN_SOURCE = REPOSITORY_ROOT / "tools" / "semconv_codegen"


@pytest.mark.parametrize(
    ("source_root", "excluded_parts", "forbidden_prefixes"),
    [
        (
            SEMANTIC_SOURCE,
            {"gremlin"},
            {"gremlin_python", "opentelemetry", "tools", "yaml"},
        ),
        (
            SEMANTIC_SOURCE / "gremlin",
            set[str](),
            {"opentelemetry", "tools", "yaml"},
        ),
        (
            CODEGEN_SOURCE,
            {"tests", "upstream"},
            {"extended_otel_semconv", "gremlin_python", "opentelemetry"},
        ),
    ],
    ids=["semantic-core", "semantic-gremlin", "codegen"],
)
def test_dependency_direction(
    source_root: Path,
    excluded_parts: set[str],
    forbidden_prefixes: set[str],
) -> None:
    violations: list[str] = []
    for source_file in source_root.rglob("*.py"):
        if excluded_parts & set(source_file.relative_to(source_root).parts):
            continue
        tree = ast.parse(source_file.read_text(encoding="utf-8"), filename=str(source_file))
        for module in _imported_modules(tree):
            if any(module == prefix or module.startswith(f"{prefix}.") for prefix in forbidden_prefixes):
                violations.append(f"{source_file.relative_to(source_root)}: {module}")

    assert not violations, "dependency boundary crossed:\n" + "\n".join(sorted(violations))


def _imported_modules(tree: ast.AST) -> Iterable[str]:
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            yield node.module
