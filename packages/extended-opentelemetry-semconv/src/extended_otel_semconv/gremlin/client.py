"""Typed GraphBinary client for semantic vertex and edge traversals."""

# gremlin-python does not publish complete type information for its dynamic traversal API.
# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Literal, cast

from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import GraphTraversal, GraphTraversalSource

from extended_otel_semconv.edges import SemanticEdge, semantic_edge_from_data
from extended_otel_semconv.entities import SemanticEntity, entity_from_attributes
from extended_otel_semconv.errors import SemanticModelError

type SemanticGraphElement = SemanticEntity | SemanticEdge
type TraversalBuilder = Callable[[GraphTraversalSource], GraphTraversal]
type TraverserKind = Literal["vertex", "edge"]

_START_STEPS: dict[str, TraverserKind] = {"V": "vertex", "E": "edge"}
_NAVIGATION_STEPS: dict[str, TraverserKind] = {
    "out": "vertex",
    "in": "vertex",
    "both": "vertex",
    "outE": "edge",
    "inE": "edge",
    "bothE": "edge",
    "outV": "vertex",
    "inV": "vertex",
    "bothV": "vertex",
    "otherV": "vertex",
}
_PRESERVING_STEPS = frozenset(
    {
        "as",
        "and",
        "barrier",
        "by",
        "coin",
        "cyclicPath",
        "dedup",
        "filter",
        "has",
        "hasId",
        "hasKey",
        "hasLabel",
        "hasNot",
        "hasValue",
        "identity",
        "is",
        "limit",
        "not",
        "or",
        "order",
        "range",
        "sample",
        "shuffle",
        "simplePath",
        "skip",
        "tail",
        "timeLimit",
        "where",
    }
)
_TRANSFORMING_STEPS = frozenset(
    {
        "cap",
        "constant",
        "count",
        "elementMap",
        "flatMap",
        "fold",
        "group",
        "groupCount",
        "id",
        "key",
        "label",
        "map",
        "match",
        "math",
        "max",
        "mean",
        "min",
        "path",
        "project",
        "properties",
        "propertyMap",
        "sack",
        "select",
        "sum",
        "tree",
        "unfold",
        "value",
        "valueMap",
        "values",
    }
)
_MUTATING_STEPS = frozenset({"addE", "addV", "drop", "mergeE", "mergeV", "property"})


class SemanticGremlinError(RuntimeError):
    """Base error for the typed semantic Gremlin client."""


class InvalidSemanticQueryError(SemanticGremlinError):
    """Raised when a query callback does not produce an unexecuted traversal."""


class UnsupportedSemanticTraversalError(SemanticGremlinError):
    """Raised when a traversal does not preserve vertex or edge results."""


class SemanticGremlinQueryError(SemanticGremlinError):
    """Raised when Gremlin Server cannot execute a supported traversal."""


class SemanticGremlinResultError(SemanticGremlinError):
    """Raised when a returned element cannot be reconstructed semantically."""


class SemanticGremlinClient:
    """Own a GraphBinary connection and return generated semantic models."""

    def __init__(self, url: str, *, traversal_source: str = "g") -> None:
        self._connection = DriverRemoteConnection(url, traversal_source)
        self._source = traversal().with_(self._connection)
        self._closed = False

    def __enter__(self) -> SemanticGremlinClient:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._connection.close()
        self._closed = True

    def query(self, build: TraversalBuilder) -> list[SemanticGraphElement]:
        if self._closed:
            raise InvalidSemanticQueryError("the semantic Gremlin client is closed")
        candidate_value = cast(object, build(self._source))
        if not isinstance(candidate_value, GraphTraversal):
            raise InvalidSemanticQueryError(
                "query callback must return an unexecuted GraphTraversal; do not call to_list(), next(), or iterate()"
            )
        candidate = candidate_value
        _validate_element_traversal(candidate)
        try:
            raw_results = candidate.element_map().to_list()
        except Exception as error:
            raise SemanticGremlinQueryError(f"Gremlin Server rejected the semantic element query: {error}") from error
        try:
            return [_semantic_element_from_map(result) for result in raw_results]
        except SemanticGremlinResultError:
            raise
        except SemanticModelError as error:
            raise SemanticGremlinResultError(f"cannot reconstruct returned semantic element: {error}") from error


def _validate_element_traversal(candidate: GraphTraversal) -> TraverserKind:
    instructions = candidate.bytecode.step_instructions
    if not instructions:
        raise InvalidSemanticQueryError("query callback returned an empty traversal")
    kind: TraverserKind | None = None
    for position, instruction in enumerate(instructions):
        if not instruction or not isinstance(instruction[0], str):
            raise UnsupportedSemanticTraversalError(
                f"traversal instruction {position} is malformed; typed queries require vertex or edge traversers"
            )
        step = instruction[0]
        if position == 0:
            kind = _START_STEPS.get(step)
            if kind is None:
                raise _unsupported_step(step, position)
            continue
        if step in _NAVIGATION_STEPS:
            kind = _NAVIGATION_STEPS[step]
            continue
        if step in _PRESERVING_STEPS:
            continue
        raise _unsupported_step(step, position)
    assert kind is not None
    return kind


def _unsupported_step(step: str, position: int) -> UnsupportedSemanticTraversalError:
    if step in _TRANSFORMING_STEPS:
        reason = "transforms elements into scalar, map, aggregate, path, or projected results"
    elif step in _MUTATING_STEPS:
        reason = "mutates graph state"
    else:
        reason = "has ambiguous or unsupported result semantics"
    return UnsupportedSemanticTraversalError(
        f"Gremlin step {step!r} at position {position} {reason}; "
        "SemanticGremlinClient requires final vertex or edge traversers. "
        "Use gremlin-python directly for raw results."
    )


def _semantic_element_from_map(value: object) -> SemanticGraphElement:
    if not isinstance(value, Mapping):
        raise SemanticGremlinResultError(
            f"Gremlin elementMap() returned {type(value).__name__}, expected a property mapping"
        )
    item = cast(Mapping[object, object], value)
    element_id = _required_string(item, "element_id")
    semantic_type = _required_string(item, "semantic_type")
    attributes = _required_object_map(item, "attributes")
    has_source = "source_id" in item
    has_target = "target_id" in item
    if has_source != has_target:
        raise SemanticGremlinResultError("edge result must contain both source_id and target_id")
    if not has_source:
        return entity_from_attributes(semantic_type, attributes, expected_id=element_id)
    source_id = _required_string(item, "source_id")
    target_id = _required_string(item, "target_id")
    return semantic_edge_from_data(
        semantic_type,
        source_id,
        target_id,
        attributes=attributes,
        expected_id=element_id,
    )


def _required_string(item: Mapping[object, object], name: str) -> str:
    value = item.get(name)
    if not isinstance(value, str) or not value:
        raise SemanticGremlinResultError(f"Gremlin element is missing non-empty string property {name!r}")
    return value


def _required_object_map(item: Mapping[object, object], name: str) -> dict[str, object]:
    value = item.get(name)
    if not isinstance(value, Mapping):
        raise SemanticGremlinResultError(f"Gremlin element is missing object property {name!r}")
    result: dict[str, object] = {}
    for key, field_value in cast(Mapping[object, object], value).items():
        if not isinstance(key, str):
            raise SemanticGremlinResultError(f"Gremlin element property {name!r} contains a non-string key")
        result[key] = field_value
    return result



__all__ = [
    "InvalidSemanticQueryError",
    "SemanticGraphElement",
    "SemanticGremlinClient",
    "SemanticGremlinError",
    "SemanticGremlinQueryError",
    "SemanticGremlinResultError",
    "UnsupportedSemanticTraversalError",
]
