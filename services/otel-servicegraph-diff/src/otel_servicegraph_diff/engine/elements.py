"""Authoritative graph elements and contributor-aware lifecycle state."""

from __future__ import annotations

import hashlib
import json
from time import time
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, StringConstraints, model_validator

from extended_otel_semconv.edges import MetricValue
from extended_otel_semconv.edges import edge_id as edge_id

SCHEMA_VERSION = "2.0"
GRAPH_ELEMENT_EVENT_TYPE = "graph_element_state_changed"
GRAPH_REQUEST_TOTAL = "service_graph.request.total"
GRAPH_REQUEST_FAILED_TOTAL = "service_graph.request.failed.total"

NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
UnixNano = Annotated[int, Field(gt=0)]
UnixMilli = Annotated[int, Field(ge=0)]
MetricDelta = Annotated[StrictInt, Field(ge=0)] | Annotated[StrictFloat, Field(ge=0, allow_inf_nan=False)]
type ExpiryClock = Literal["event_time", "processing_time"]


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class GraphNode(FrozenModel):
    kind: Literal["node"] = "node"
    id: NonEmptyString
    type: NonEmptyString
    attributes: dict[str, object] = Field(default_factory=dict)


class GraphEdge(FrozenModel):
    kind: Literal["edge"] = "edge"
    id: NonEmptyString
    type: NonEmptyString
    source_id: NonEmptyString
    target_id: NonEmptyString
    attributes: dict[str, object] = Field(default_factory=dict)
    metrics: dict[str, MetricValue] = Field(default_factory=dict)


type GraphElement = Annotated[GraphNode | GraphEdge, Field(discriminator="kind")]


class GraphElementEventBase(FrozenModel):
    schema_version: Literal["2.0"] = SCHEMA_VERSION
    event_id: NonEmptyString
    event_type: Literal["graph_element_state_changed"] = GRAPH_ELEMENT_EVENT_TYPE
    element_id: NonEmptyString
    observed_at_unix_nano: UnixNano
    emitted_at_unix_ms: UnixMilli


class GraphElementUpsertEvent(GraphElementEventBase):
    operation: Literal["upsert"] = "upsert"
    payload_hash: NonEmptyString
    element: GraphElement


class GraphElementDeleteEvent(GraphElementEventBase):
    operation: Literal["delete"] = "delete"
    payload_hash: None = None
    element: None = None


type GraphElementEvent = Annotated[
    GraphElementUpsertEvent | GraphElementDeleteEvent,
    Field(discriminator="operation"),
]


class GraphContribution(FrozenModel):
    operation: Literal["upsert"] = "upsert"
    contributor_id: NonEmptyString
    observed_at_unix_nano: UnixNano
    element: GraphElement
    metric_deltas: dict[str, MetricDelta] = Field(default_factory=dict)
    ttl_seconds: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_metric_deltas(self) -> GraphContribution:
        if isinstance(self.element, GraphNode) and self.metric_deltas:
            raise ValueError("node contributions cannot contain metric deltas")
        return self


class GraphContributionRetraction(FrozenModel):
    operation: Literal["retract"] = "retract"
    contributor_id: NonEmptyString
    element_id: NonEmptyString
    observed_at_unix_nano: UnixNano


type GraphElementMutation = Annotated[
    GraphContribution | GraphContributionRetraction,
    Field(discriminator="operation"),
]


class ContributorSnapshot(FrozenModel):
    observed_at_unix_nano: UnixNano
    event_expires_at_unix_nano: UnixNano | None = None
    processing_expires_at_unix_ms: UnixMilli | None = None
    element: GraphElement


class GraphElementState(FrozenModel):
    element_id: NonEmptyString
    contributors: dict[str, ContributorSnapshot]
    metrics: dict[str, MetricValue] = Field(default_factory=dict)
    last_payload_hash: NonEmptyString


class GraphElementLifecycleResult(FrozenModel):
    state: GraphElementState | None
    event: GraphElementEvent | None = None


def apply_contribution(
    previous: GraphElementState | None,
    contribution: GraphContribution,
    *,
    ttl_seconds: int,
    event_expiry_base_unix_nano: int | None = None,
    processing_time_unix_ms: int,
    emitted_at_unix_ms: int | None = None,
) -> GraphElementLifecycleResult:
    if contribution.ttl_seconds is None:
        if ttl_seconds <= 0:
            raise ValueError("default contributor TTL must be greater than zero")
        effective_ttl_seconds = ttl_seconds
    else:
        effective_ttl_seconds = contribution.ttl_seconds
    if effective_ttl_seconds == 0:
        event_expiry = None
        processing_expiry = None
    else:
        ttl_nanoseconds = effective_ttl_seconds * 1_000_000_000
        ttl_milliseconds = effective_ttl_seconds * 1_000
        event_expiry_base = max(contribution.observed_at_unix_nano, event_expiry_base_unix_nano or 0)
        event_expiry = event_expiry_base + ttl_nanoseconds
        processing_expiry = processing_time_unix_ms + ttl_milliseconds
    element_id = contribution.element.id
    existing = previous.contributors.get(contribution.contributor_id) if previous is not None else None
    if existing is not None and contribution.observed_at_unix_nano < existing.observed_at_unix_nano:
        return GraphElementLifecycleResult(state=previous)
    if previous is not None:
        _validate_element_identity(previous, contribution.element)

    contributors = dict(previous.contributors) if previous is not None else {}
    contributors[contribution.contributor_id] = ContributorSnapshot(
        observed_at_unix_nano=contribution.observed_at_unix_nano,
        event_expires_at_unix_nano=event_expiry,
        processing_expires_at_unix_ms=processing_expiry,
        element=contribution.element,
    )
    metrics = dict(previous.metrics) if previous is not None else {}
    for name, delta in contribution.metric_deltas.items():
        metrics[name] = metrics.get(name, 0) + delta
    return _updated_state(
        previous,
        element_id,
        contributors,
        metrics,
        contribution.observed_at_unix_nano,
        emitted_at_unix_ms,
    )


def retract_contribution(
    previous: GraphElementState | None,
    retraction: GraphContributionRetraction,
    *,
    emitted_at_unix_ms: int | None = None,
) -> GraphElementLifecycleResult:
    if previous is None:
        return GraphElementLifecycleResult(state=None)
    if previous.element_id != retraction.element_id:
        raise ValueError(
            f"retraction for {retraction.element_id!r} reached state for {previous.element_id!r}"
        )
    snapshot = previous.contributors.get(retraction.contributor_id)
    if snapshot is None or retraction.observed_at_unix_nano < snapshot.observed_at_unix_nano:
        return GraphElementLifecycleResult(state=previous)

    contributors = dict(previous.contributors)
    del contributors[retraction.contributor_id]
    if contributors:
        return _updated_state(
            previous,
            previous.element_id,
            contributors,
            previous.metrics,
            retraction.observed_at_unix_nano,
            emitted_at_unix_ms,
        )

    emitted_at = emitted_at_unix_ms if emitted_at_unix_ms is not None else int(time() * 1000)
    return GraphElementLifecycleResult(
        state=None,
        event=_delete_event(previous.element_id, retraction.observed_at_unix_nano, emitted_at),
    )


def expire_contributors(
    previous: GraphElementState,
    *,
    clock: ExpiryClock,
    timestamp: int,
    emitted_at_unix_ms: int | None = None,
) -> GraphElementLifecycleResult:
    expired = {
        contributor_id: snapshot
        for contributor_id, snapshot in previous.contributors.items()
        if _snapshot_expired(snapshot, clock, timestamp)
    }
    if not expired:
        return GraphElementLifecycleResult(state=previous)

    contributors = {
        contributor_id: snapshot
        for contributor_id, snapshot in previous.contributors.items()
        if contributor_id not in expired
    }
    observed_at = max(
        snapshot.event_expires_at_unix_nano or snapshot.observed_at_unix_nano
        for snapshot in expired.values()
    )
    if not contributors:
        emitted_at = emitted_at_unix_ms if emitted_at_unix_ms is not None else int(time() * 1000)
        return GraphElementLifecycleResult(
            state=None,
            event=_delete_event(previous.element_id, observed_at, emitted_at),
        )
    return _updated_state(
        previous,
        previous.element_id,
        contributors,
        previous.metrics,
        observed_at,
        emitted_at_unix_ms,
    )


def payload_hash(element: GraphElement) -> str:
    return _digest(element.model_dump(mode="json"))


def _updated_state(
    previous: GraphElementState | None,
    element_id: str,
    contributors: dict[str, ContributorSnapshot],
    metrics: dict[str, MetricValue],
    observed_at_unix_nano: int,
    emitted_at_unix_ms: int | None,
) -> GraphElementLifecycleResult:
    element = _merge_element(element_id, contributors, metrics)
    current_hash = payload_hash(element)
    state = GraphElementState(
        element_id=element_id,
        contributors=contributors,
        metrics=metrics,
        last_payload_hash=current_hash,
    )
    if previous is not None and previous.last_payload_hash == current_hash:
        return GraphElementLifecycleResult(state=state)
    emitted_at = emitted_at_unix_ms if emitted_at_unix_ms is not None else int(time() * 1000)
    return GraphElementLifecycleResult(
        state=state,
        event=_upsert_event(element, observed_at_unix_nano, emitted_at, current_hash),
    )


def _merge_element(
    element_id: str,
    contributors: dict[str, ContributorSnapshot],
    metrics: dict[str, MetricValue],
) -> GraphElement:
    ordered = sorted(
        contributors.items(),
        key=lambda item: (-item[1].observed_at_unix_nano, item[0]),
    )
    winner = ordered[0][1].element
    attributes: dict[str, object] = {}
    for _, snapshot in ordered:
        for name, value in snapshot.element.attributes.items():
            attributes.setdefault(name, value)
    if isinstance(winner, GraphNode):
        return GraphNode(id=element_id, type=winner.type, attributes=attributes)
    return GraphEdge(
        id=element_id,
        type=winner.type,
        source_id=winner.source_id,
        target_id=winner.target_id,
        attributes=attributes,
        metrics=dict(sorted(metrics.items())),
    )


def _validate_element_identity(state: GraphElementState, element: GraphElement) -> None:
    existing = next(iter(state.contributors.values())).element
    match existing, element:
        case GraphNode(type=existing_type), GraphNode(type=current_type) if existing_type == current_type:
            return
        case (
            GraphEdge(type=existing_type, source_id=existing_source, target_id=existing_target),
            GraphEdge(type=current_type, source_id=current_source, target_id=current_target),
        ) if (existing_type, existing_source, existing_target) == (
            current_type,
            current_source,
            current_target,
        ):
            return
        case _:
            raise ValueError(f"contribution conflicts with graph element identity {state.element_id!r}")


def _snapshot_expired(snapshot: ContributorSnapshot, clock: ExpiryClock, timestamp: int) -> bool:
    match clock:
        case "event_time":
            return (
                snapshot.event_expires_at_unix_nano is not None
                and snapshot.event_expires_at_unix_nano <= timestamp
            )
        case "processing_time":
            return (
                snapshot.processing_expires_at_unix_ms is not None
                and snapshot.processing_expires_at_unix_ms <= timestamp
            )


def _upsert_event(
    element: GraphElement,
    observed_at_unix_nano: int,
    emitted_at_unix_ms: int,
    current_hash: str,
) -> GraphElementUpsertEvent:
    return GraphElementUpsertEvent(
        event_id=_event_id("upsert", element.id, observed_at_unix_nano, current_hash),
        element_id=element.id,
        observed_at_unix_nano=observed_at_unix_nano,
        emitted_at_unix_ms=emitted_at_unix_ms,
        payload_hash=current_hash,
        element=element,
    )


def _delete_event(
    element_id: str,
    observed_at_unix_nano: int,
    emitted_at_unix_ms: int,
) -> GraphElementDeleteEvent:
    return GraphElementDeleteEvent(
        event_id=_event_id("delete", element_id, observed_at_unix_nano, None),
        element_id=element_id,
        observed_at_unix_nano=observed_at_unix_nano,
        emitted_at_unix_ms=emitted_at_unix_ms,
    )


def _event_id(operation: str, element_id: str, observed_at_unix_nano: int, current_hash: str | None) -> str:
    return _digest(
        {
            "operation": operation,
            "element_id": element_id,
            "observed_at_unix_nano": observed_at_unix_nano,
            "payload_hash": current_hash,
        }
    )


def _digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
