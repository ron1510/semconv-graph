# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import Mock

import pytest
from gremlin_python.process.graph_traversal import GraphTraversal, GraphTraversalSource, __
from gremlin_python.process.traversal import Bytecode, TraversalStrategies
from gremlin_python.structure.graph import Graph

import extended_otel_semconv.gremlin.client as gremlin_module
from extended_otel_semconv import Service, ServiceCallsServiceEdge
from extended_otel_semconv.edges import edge_id
from extended_otel_semconv.gremlin import (
    InvalidSemanticQueryError,
    SemanticGremlinClient,
    SemanticGremlinQueryError,
    SemanticGremlinResultError,
    UnsupportedSemanticTraversalError,
)
from extended_otel_semconv.gremlin.client import (
    _semantic_element_from_map,
    _validate_element_traversal,
)


@pytest.fixture
def source() -> GraphTraversalSource:
    return GraphTraversalSource(Graph(), TraversalStrategies())


@pytest.mark.parametrize(
    ("build", "kind"),
    [
        (lambda g: g.V().has_label("service").has("service_name", "checkout").limit(5), "vertex"),
        (lambda g: g.V().out("calls").in_("calls").both("calls"), "vertex"),
        (lambda g: g.V().out_e("calls").in_v(), "vertex"),
        (lambda g: g.V().in_e("calls").out_v().both_e("calls"), "edge"),
        (lambda g: g.E().other_v(), "vertex"),
        (lambda g: g.V().where(__.out("calls")).not_(__.has("disabled", True)), "vertex"),
        (lambda g: g.V().order().by("service_name").dedup().range_(0, 10).tail(2), "vertex"),
    ],
)
def test_element_preserving_traversals_are_supported(
    source: GraphTraversalSource,
    build: Callable[[GraphTraversalSource], GraphTraversal],
    kind: str,
) -> None:
    assert _validate_element_traversal(build(source)) == kind


@pytest.mark.parametrize(
    "step",
    [
        "values",
        "valueMap",
        "elementMap",
        "properties",
        "id",
        "label",
        "count",
        "sum",
        "min",
        "max",
        "mean",
        "fold",
        "project",
        "group",
        "groupCount",
        "path",
        "tree",
        "select",
        "match",
        "map",
        "flatMap",
    ],
)
def test_transforming_traversals_fail_with_the_offending_step(
    source: GraphTraversalSource,
    step: str,
) -> None:
    candidate = source.V()
    candidate.bytecode.add_step(step)

    with pytest.raises(UnsupportedSemanticTraversalError, match=repr(step)):
        _validate_element_traversal(candidate)


def test_mutating_unknown_empty_and_malformed_traversals_are_rejected(source: GraphTraversalSource) -> None:
    with pytest.raises(UnsupportedSemanticTraversalError, match="mutates graph state"):
        _validate_element_traversal(source.add_v("service"))

    unknown = source.V()
    unknown.bytecode.add_step("customProviderStep")
    with pytest.raises(UnsupportedSemanticTraversalError, match="ambiguous or unsupported"):
        _validate_element_traversal(unknown)

    with pytest.raises(InvalidSemanticQueryError, match="empty traversal"):
        _validate_element_traversal(GraphTraversal(Graph(), TraversalStrategies(), Bytecode()))


def test_element_maps_reconstruct_concrete_entities_and_edges() -> None:
    service = _semantic_element_from_map(
        {
            "element_id": "service:checkout",
            "semantic_type": "service",
            "attributes": {"service.name": "checkout", "service.version": "1.4.0"},
        }
    )
    expected_edge_id = edge_id("service:storefront", "calls", "service:checkout")
    edge = _semantic_element_from_map(
        {
            "element_id": expected_edge_id,
            "semantic_type": "calls",
            "source_id": "service:storefront",
            "target_id": "service:checkout",
            "attributes": {},
        }
    )

    assert isinstance(service, Service)
    assert service.service_version == "1.4.0"
    assert isinstance(edge, ServiceCallsServiceEdge)


@pytest.mark.parametrize(
    "value",
    [
        "not-a-map",
        {},
        {"element_id": "service:one", "semantic_type": "service", "attributes": []},
        {
            "element_id": "edge:one",
            "semantic_type": "calls",
            "source_id": "service:one",
            "attributes": {},
        },
    ],
)
def test_malformed_element_maps_fail_clearly(value: object) -> None:
    with pytest.raises(SemanticGremlinResultError):
        _semantic_element_from_map(value)


def test_client_hydrates_results_and_owns_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = Mock()
    monkeypatch.setattr(gremlin_module, "DriverRemoteConnection", Mock(return_value=connection))
    observed_steps: list[list[list[object]]] = []

    def execute(candidate: GraphTraversal) -> list[dict[str, object]]:
        observed_steps.append(candidate.bytecode.step_instructions)
        return [
            {
                "element_id": "service:checkout",
                "semantic_type": "service",
                "attributes": {"service.name": "checkout"},
            }
        ]

    monkeypatch.setattr(GraphTraversal, "to_list", execute)

    with SemanticGremlinClient("ws://gremlin:8182/gremlin") as client:
        results = client.query(lambda g: g.V().has_label("service"))
        assert isinstance(results[0], Service)
        assert observed_steps[0][-1] == ["elementMap"]
    connection.close.assert_called_once_with()

    client.close()
    connection.close.assert_called_once_with()
    with pytest.raises(InvalidSemanticQueryError, match="closed"):
        client.query(lambda g: g.V())


def test_client_rejects_executed_callbacks_before_submission(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gremlin_module, "DriverRemoteConnection", Mock(return_value=Mock()))
    monkeypatch.setattr(GraphTraversal, "to_list", lambda _: [])
    client = SemanticGremlinClient("ws://gremlin:8182/gremlin")

    with pytest.raises(InvalidSemanticQueryError, match="unexecuted GraphTraversal"):
        client.query(lambda g: g.V().to_list())  # type: ignore[arg-type,return-value]
    client.close()


def test_client_preserves_execution_and_model_errors_as_causes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gremlin_module, "DriverRemoteConnection", Mock(return_value=Mock()))
    client = SemanticGremlinClient("ws://gremlin:8182/gremlin")
    server_error = RuntimeError("server unavailable")

    def fail(_: GraphTraversal) -> list[object]:
        raise server_error

    monkeypatch.setattr(GraphTraversal, "to_list", fail)
    with pytest.raises(SemanticGremlinQueryError) as execution:
        client.query(lambda g: g.V())
    assert execution.value.__cause__ is server_error

    monkeypatch.setattr(
        GraphTraversal,
        "to_list",
        lambda _: [
            {
                "element_id": "service:wrong",
                "semantic_type": "service",
                "attributes": {"service.name": "checkout"},
            }
        ],
    )
    with pytest.raises(SemanticGremlinResultError) as result:
        client.query(lambda g: g.V())
    assert result.value.__cause__ is not None
    client.close()
