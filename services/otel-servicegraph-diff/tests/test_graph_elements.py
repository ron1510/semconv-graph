from __future__ import annotations

import pytest
from pydantic import TypeAdapter, ValidationError

from otel_servicegraph_diff.engine.elements import (
    GRAPH_REQUEST_FAILED_TOTAL,
    GRAPH_REQUEST_TOTAL,
    GraphContribution,
    GraphContributionRetraction,
    GraphEdge,
    GraphElementEvent,
    GraphElementLifecycleResult,
    GraphElementState,
    GraphElementUpsertEvent,
    GraphNode,
    apply_contribution,
    edge_id,
    expire_contributors,
    retract_contribution,
)

TTL_SECONDS = 5


def test_node_contributors_complete_each_others_optional_attributes() -> None:
    first = _apply(None, _node_contribution("a", 10, {"zone": "eu-1"}))
    second = _apply(first.state, _node_contribution("b", 20, {"version": "2.4"}))

    assert isinstance(second.event, GraphElementUpsertEvent)
    assert isinstance(second.event.element, GraphNode)
    assert second.event.element.attributes == {"version": "2.4", "zone": "eu-1"}


def test_node_attribute_conflicts_use_timestamp_then_contributor_id() -> None:
    state = _apply(None, _node_contribution("z", 10, {"version": "old"})).state
    state = _apply(state, _node_contribution("z", 20, {"version": "new"})).state
    result = _apply(state, _node_contribution("a", 20, {"version": "tie-winner"}))

    assert isinstance(result.event, GraphElementUpsertEvent)
    assert isinstance(result.event.element, GraphNode)
    assert result.event.element.attributes["version"] == "tie-winner"


def test_partial_expiry_falls_back_and_final_expiry_deletes() -> None:
    first = _apply(None, _node_contribution("a", 10, {"zone": "eu-1", "version": "1"}))
    second = _apply(first.state, _node_contribution("b", 20, {"version": "2"}))
    assert second.state is not None

    fallback = expire_contributors(
        second.state,
        clock="event_time",
        timestamp=_event_expiry(10),
        emitted_at_unix_ms=103,
    )
    assert isinstance(fallback.event, GraphElementUpsertEvent)
    assert isinstance(fallback.event.element, GraphNode)
    assert fallback.event.element.attributes == {"version": "2"}

    assert fallback.state is not None
    deleted = expire_contributors(
        fallback.state,
        clock="event_time",
        timestamp=_event_expiry(20),
        emitted_at_unix_ms=104,
    )
    assert deleted.state is None
    assert deleted.event is not None
    assert deleted.event.operation == "delete"
    assert deleted.event.element is None


def test_field_survives_while_another_contributor_still_supplies_it() -> None:
    first = _apply(None, _node_contribution("a", 10, {"zone": "eu-1"}))
    second = _apply(first.state, _node_contribution("b", 20, {"zone": "eu-1"}))
    assert second.state is not None

    result = expire_contributors(second.state, clock="event_time", timestamp=_event_expiry(10))

    assert result.state is not None
    assert set(result.state.contributors) == {"b"}
    assert result.event is None


def test_refresh_makes_old_event_and_processing_timers_stale() -> None:
    first = _apply(None, _node_contribution("a", 1_000_000_000, {"version": "1"}), processing_time=100)
    refreshed = _apply(
        first.state,
        _node_contribution("a", 2_000_000_000, {"version": "2"}),
        processing_time=200,
    )
    assert refreshed.state is not None

    old_event = expire_contributors(
        refreshed.state,
        clock="event_time",
        timestamp=_event_expiry(1_000_000_000),
    )
    old_processing = expire_contributors(
        refreshed.state,
        clock="processing_time",
        timestamp=100 + TTL_SECONDS * 1_000,
    )

    assert old_event.state == refreshed.state
    assert old_event.event is None
    assert old_processing.state == refreshed.state
    assert old_processing.event is None


def test_processing_time_expires_contributor_when_event_time_is_idle() -> None:
    active = _apply(
        None,
        _node_contribution("a", 1_000_000_000, {"version": "1"}),
        processing_time=20_000,
    )
    assert active.state is not None

    deleted = expire_contributors(
        active.state,
        clock="processing_time",
        timestamp=25_000,
        emitted_at_unix_ms=25_000,
    )

    assert deleted.state is None
    assert deleted.event is not None
    assert deleted.event.operation == "delete"
    assert deleted.event.observed_at_unix_nano == _event_expiry(1_000_000_000)


def test_one_timer_can_expire_multiple_contributors_with_one_event() -> None:
    first = _apply(None, _node_contribution("a", 10, {"zone": "eu"}), processing_time=100)
    second = _apply(first.state, _node_contribution("b", 20, {"version": "2"}), processing_time=100)
    assert second.state is not None

    result = expire_contributors(
        second.state,
        clock="processing_time",
        timestamp=100 + TTL_SECONDS * 1_000,
    )

    assert result.state is None
    assert result.event is not None
    assert result.event.operation == "delete"


def test_edge_metrics_accumulate_without_decreasing_on_partial_expiry() -> None:
    edge = _edge()
    first = _apply(None, _edge_contribution(edge, "a", 10, requests=3, failures=1))
    second = _apply(first.state, _edge_contribution(edge, "b", 20, requests=5, failures=0))

    assert second.state is not None
    assert second.state.metrics == {
        GRAPH_REQUEST_FAILED_TOTAL: 1,
        GRAPH_REQUEST_TOTAL: 8,
    }
    partial = expire_contributors(second.state, clock="event_time", timestamp=_event_expiry(10))
    assert partial.state is not None
    assert partial.state.metrics == second.state.metrics
    assert partial.event is None


def test_edge_delete_and_recreation_reset_metric_lifetime() -> None:
    edge = _edge()
    active = _apply(None, _edge_contribution(edge, "a", 10, requests=7))
    assert active.state is not None
    deleted = expire_contributors(active.state, clock="event_time", timestamp=_event_expiry(10))
    recreated = _apply(deleted.state, _edge_contribution(edge, "b", 30, requests=2))

    assert recreated.state is not None
    assert recreated.state.metrics == {GRAPH_REQUEST_TOTAL: 2}


def test_explicit_retraction_recomputes_shared_element_and_deletes_final_contributor() -> None:
    first = _apply(None, _node_contribution("a", 10, {"zone": "eu", "version": "1"}))
    second = _apply(first.state, _node_contribution("b", 20, {"version": "2"}))

    partial = retract_contribution(
        second.state,
        GraphContributionRetraction(
            contributor_id="a",
            element_id="k8s.pod:pod-1",
            observed_at_unix_nano=30,
        ),
        emitted_at_unix_ms=101,
    )
    assert partial.state is not None
    assert isinstance(partial.event, GraphElementUpsertEvent)
    assert isinstance(partial.event.element, GraphNode)
    assert partial.event.element.attributes == {"version": "2"}

    deleted = retract_contribution(
        partial.state,
        GraphContributionRetraction(
            contributor_id="b",
            element_id="k8s.pod:pod-1",
            observed_at_unix_nano=40,
        ),
        emitted_at_unix_ms=102,
    )
    assert deleted.state is None
    assert deleted.event is not None
    assert deleted.event.operation == "delete"


def test_stale_or_unknown_retraction_is_idempotent() -> None:
    active = _apply(None, _node_contribution("a", 20, {"version": "2"}))
    stale = retract_contribution(
        active.state,
        GraphContributionRetraction(
            contributor_id="a",
            element_id="k8s.pod:pod-1",
            observed_at_unix_nano=10,
        ),
    )
    unknown = retract_contribution(
        active.state,
        GraphContributionRetraction(
            contributor_id="missing",
            element_id="k8s.pod:pod-1",
            observed_at_unix_nano=30,
        ),
    )

    assert stale.state is active.state
    assert stale.event is None
    assert unknown.state is active.state
    assert unknown.event is None


def test_retraction_rejects_mismatched_element_state() -> None:
    active = _apply(None, _node_contribution("a", 20, {}))

    with pytest.raises(ValueError, match="retraction for"):
        retract_contribution(
            active.state,
            GraphContributionRetraction(
                contributor_id="a",
                element_id="service:other",
                observed_at_unix_nano=30,
            ),
        )


def test_contribution_specific_ttl_overrides_engine_default() -> None:
    contribution = _node_contribution("a", 1_000_000_000, {}).model_copy(
        update={"ttl_seconds": 30}
    )
    result = _apply(None, contribution, processing_time=100)

    assert result.state is not None
    snapshot = result.state.contributors["a"]
    assert snapshot.event_expires_at_unix_nano == 31_000_000_000
    assert snapshot.processing_expires_at_unix_ms == 30_100


def test_older_contributor_observation_is_ignored_without_refreshing_expiry() -> None:
    first = _apply(None, _node_contribution("a", 20, {"version": "new"}), processing_time=200)
    late = _apply(first.state, _node_contribution("a", 10, {"version": "old"}), processing_time=300)

    assert late.state is first.state
    assert late.event is None


def test_state_round_trip_preserves_contributor_expiry() -> None:
    result = _apply(None, _node_contribution("a", 10, {"zone": "eu"}), processing_time=100)
    assert result.state is not None

    restored = GraphElementState.model_validate_json(result.state.model_dump_json())

    assert restored == result.state
    assert restored.contributors["a"].event_expires_at_unix_nano == _event_expiry(10)
    assert restored.contributors["a"].processing_expires_at_unix_ms == 5_100


def test_event_contract_round_trips_as_discriminated_union() -> None:
    result = _apply(
        None,
        _node_contribution("a", 10, {"zone": "eu-1"}),
        emitted_at=100,
    )
    assert result.event is not None

    adapter: TypeAdapter[GraphElementEvent] = TypeAdapter(GraphElementEvent)
    restored = adapter.validate_json(result.event.model_dump_json())

    assert restored == result.event
    assert restored.schema_version == "2.0"
    assert restored.event_type == "graph_element_state_changed"
    assert restored.element_id == "k8s.pod:pod-1"


def test_contribution_validates_metric_identity() -> None:
    node = GraphNode(id="service:a", type="service")
    with pytest.raises(ValidationError, match="node contributions"):
        GraphContribution(
            contributor_id="a",
            observed_at_unix_nano=1,
            element=node,
            metric_deltas={GRAPH_REQUEST_TOTAL: 1},
        )
    edge = _edge()
    with pytest.raises(ValidationError):
        GraphContribution(
            contributor_id="a",
            observed_at_unix_nano=1,
            element=edge,
            metric_deltas={GRAPH_REQUEST_TOTAL: -1},
        )


def test_contribution_requires_positive_ttl() -> None:
    with pytest.raises(ValueError, match="TTL must be greater than zero"):
        apply_contribution(
            None,
            _node_contribution("a", 10, {}),
            ttl_seconds=0,
            processing_time_unix_ms=100,
        )


def test_same_element_id_rejects_incompatible_semantic_identity() -> None:
    active = _apply(None, _node_contribution("a", 10, {"zone": "eu"}))
    incompatible = GraphContribution(
        contributor_id="b",
        observed_at_unix_nano=20,
        element=GraphNode(id="k8s.pod:pod-1", type="service"),
    )

    with pytest.raises(ValueError, match="conflicts with graph element identity"):
        _apply(active.state, incompatible)


def _apply(
    previous: GraphElementState | None,
    contribution: GraphContribution,
    *,
    processing_time: int = 1_000,
    emitted_at: int = 100,
) -> GraphElementLifecycleResult:
    return apply_contribution(
        previous,
        contribution,
        ttl_seconds=TTL_SECONDS,
        processing_time_unix_ms=processing_time,
        emitted_at_unix_ms=emitted_at,
    )


def _node_contribution(contributor: str, observed_at: int, attributes: dict[str, object]) -> GraphContribution:
    node = GraphNode(id="k8s.pod:pod-1", type="k8s.pod", attributes=attributes)
    return GraphContribution(
        contributor_id=contributor,
        observed_at_unix_nano=observed_at,
        element=node,
    )


def _edge_contribution(
    edge: GraphEdge,
    contributor: str,
    observed_at: int,
    *,
    requests: int,
    failures: int | None = None,
) -> GraphContribution:
    deltas: dict[str, int | float] = {GRAPH_REQUEST_TOTAL: requests}
    if failures is not None:
        deltas[GRAPH_REQUEST_FAILED_TOTAL] = failures
    return GraphContribution(
        contributor_id=contributor,
        observed_at_unix_nano=observed_at,
        element=edge,
        metric_deltas=deltas,
    )


def _edge() -> GraphEdge:
    element_id = edge_id("service:a", "calls", "service:b")
    return GraphEdge(id=element_id, type="calls", source_id="service:a", target_id="service:b")


def _event_expiry(observed_at: int) -> int:
    return observed_at + TTL_SECONDS * 1_000_000_000
