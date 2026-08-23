# Framework operators are intentionally exercised through their private state seams.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import logging
from typing import cast

import pytest

pytest.importorskip("pyflink")

from pyflink.datastream.functions import KeyedProcessFunction, TimeDomain
from pyflink.datastream.state import ValueState

from otel_servicegraph_diff.engine.elements import (
    GraphContribution,
    GraphContributionRetraction,
    GraphElementState,
    GraphElementUpsertEvent,
    GraphNode,
)
from otel_servicegraph_diff.flink_job import (
    Counter,
    OnTimerProcessContext,
    ProcessContext,
    _EntityEventContributionsProcess,
    _GraphElementLifecycleProcess,
    _PayloadParser,
)
from otel_servicegraph_diff.ingest.contributions import contributions_from_servicegraph_datapoint
from otel_servicegraph_diff.ingest.entity_events import EntitySourceState, EntityStateObservation
from otel_servicegraph_diff.ingest.metrics import SERVICE_GRAPH_REQUEST_TOTAL


class FakeValueState[T]:
    def __init__(self) -> None:
        self.serialized: T | None = None

    def value(self) -> T | None:
        return self.serialized

    def update(self, value: T) -> None:
        self.serialized = value

    def clear(self) -> None:
        self.serialized = None


class FakeCounter:
    def __init__(self) -> None:
        self.value = 0

    def inc(self, n: int = 1) -> None:
        self.value += n


class FakeTimerService:
    def __init__(self, watermark: int = 0, processing_time: int = 10_000) -> None:
        self.watermark = watermark
        self.processing_time = processing_time
        self.registered_event: list[int] = []
        self.registered_processing: list[int] = []

    def current_processing_time(self) -> int:
        return self.processing_time

    def current_watermark(self) -> int:
        return self.watermark

    def register_event_time_timer(self, timestamp: int) -> None:
        self.registered_event.append(timestamp)

    def register_processing_time_timer(self, timestamp: int) -> None:
        self.registered_processing.append(timestamp)


class FakeProcessContext:
    def __init__(self, timer_service: FakeTimerService) -> None:
        self._timer_service = timer_service

    def timer_service(self) -> FakeTimerService:
        return self._timer_service


class FakeOnTimerContext(FakeProcessContext):
    def __init__(self, timer_service: FakeTimerService, time_domain: TimeDomain) -> None:
        super().__init__(timer_service)
        self._time_domain = time_domain

    def time_domain(self) -> TimeDomain:
        return self._time_domain


def test_lifecycle_operator_persists_contribution_and_registers_both_timers() -> None:
    state = FakeValueState[str]()
    timers = FakeTimerService()
    operator = _operator(state)
    contribution = _service_contribution(1_000_000_001)

    events = tuple(operator.process_element(contribution, _process_context(timers)))

    assert len(events) == 1
    assert isinstance(events[0], GraphElementUpsertEvent)
    assert timers.registered_event == [6_001]
    assert timers.registered_processing == [15_000]
    assert state.serialized is not None
    persisted = GraphElementState.model_validate_json(state.serialized)
    assert persisted.element_id == contribution.element.id
    assert set(persisted.contributors) == {contribution.contributor_id}


def test_refresh_registers_new_timers_and_old_timer_is_ignored() -> None:
    state = FakeValueState[str]()
    timers = FakeTimerService()
    operator = _operator(state)
    context = _process_context(timers)
    tuple(operator.process_element(_service_contribution(1_000_000_001, version="1"), context))

    timers.processing_time = 11_000
    tuple(operator.process_element(_service_contribution(2_000_000_001, version="2"), context))
    stale = tuple(operator.on_timer(6_001, _timer_context(timers, TimeDomain.EVENT_TIME)))

    assert timers.registered_event == [6_001, 7_001]
    assert timers.registered_processing == [15_000, 16_000]
    assert stale == ()
    assert state.serialized is not None
    persisted = GraphElementState.model_validate_json(state.serialized)
    snapshot = next(iter(persisted.contributors.values()))
    assert snapshot.observed_at_unix_nano == 2_000_000_001


def test_event_timer_deletes_final_contributor_and_clears_state() -> None:
    state = FakeValueState[str]()
    timers = FakeTimerService()
    operator = _operator(state)
    tuple(operator.process_element(_service_contribution(1_000_000_001), _process_context(timers)))

    events = tuple(operator.on_timer(6_001, _timer_context(timers, TimeDomain.EVENT_TIME)))

    assert len(events) == 1
    assert events[0].operation == "delete"
    assert state.serialized is None


def test_explicit_retraction_deletes_final_contributor_and_clears_state() -> None:
    state = FakeValueState[str]()
    timers = FakeTimerService()
    operator = _operator(state)
    contribution = _service_contribution(1_000_000_001)
    tuple(operator.process_element(contribution, _process_context(timers)))

    events = tuple(
        operator.process_element(
            GraphContributionRetraction(
                contributor_id=contribution.contributor_id,
                element_id=contribution.element.id,
                observed_at_unix_nano=2_000_000_001,
            ),
            _process_context(timers),
        )
    )

    assert len(events) == 1
    assert events[0].operation == "delete"
    assert state.serialized is None


def test_entity_source_operator_checkpoints_snapshot_reconciliation() -> None:
    state = FakeValueState[str]()
    operator = _EntityEventContributionsProcess(state_ttl_seconds=60)
    operator._state = cast(ValueState[str], state)
    contribution = _service_contribution(1_000_000_001)
    observation = EntityStateObservation(
        source_key="source",
        contributor_id=contribution.contributor_id,
        node_element_id=contribution.element.id,
        observed_at_unix_nano=contribution.observed_at_unix_nano,
        payload_hash="hash",
        contributions=(contribution,),
    )

    mutations = tuple(operator.process_element(observation, cast(KeyedProcessFunction.Context, object())))

    assert mutations == (contribution,)
    assert state.serialized is not None
    restored = EntitySourceState.model_validate_json(state.serialized)
    assert restored.element_ids == (contribution.element.id,)


def test_processing_timer_expires_when_event_time_is_idle() -> None:
    state = FakeValueState[str]()
    timers = FakeTimerService(processing_time=20_000)
    operator = _operator(state)
    tuple(operator.process_element(_service_contribution(1_000_000_001), _process_context(timers)))

    events = tuple(operator.on_timer(25_000, _timer_context(timers, TimeDomain.PROCESSING_TIME)))

    assert len(events) == 1
    assert events[0].operation == "delete"
    assert state.serialized is None


def test_watermark_extends_event_expiry_without_changing_processing_expiry() -> None:
    state = FakeValueState[str]()
    timers = FakeTimerService(watermark=4_000, processing_time=10_000)
    operator = _operator(state)

    tuple(operator.process_element(_service_contribution(1_000_000_001), _process_context(timers)))

    assert timers.registered_event == [9_000]
    assert timers.registered_processing == [15_000]


def test_empty_state_timer_emits_nothing() -> None:
    operator = _operator(FakeValueState[str]())

    assert tuple(
        operator.on_timer(
            25_000,
            _timer_context(FakeTimerService(), TimeDomain.PROCESSING_TIME),
        )
    ) == ()


def test_payload_parser_counts_warns_and_discards_rejected_inputs(caplog: pytest.LogCaptureFixture) -> None:
    parser = _PayloadParser()
    counter = FakeCounter()
    parser._rejected_inputs = cast(Counter, counter)

    with caplog.at_level(logging.WARNING):
        assert tuple(parser.flat_map("{not-json")) == ()

    assert counter.value == 1
    assert "discarding rejected servicegraph input" in caplog.text
    assert "{not-json" not in caplog.text


def test_operator_requires_runtime_initialization() -> None:
    operator = _GraphElementLifecycleProcess(ttl_seconds=5, state_ttl_seconds=60)

    with pytest.raises(RuntimeError, match="graph element state accessed before operator initialization"):
        operator._require_state()


def _operator(state: FakeValueState[str]) -> _GraphElementLifecycleProcess:
    operator = _GraphElementLifecycleProcess(ttl_seconds=5, state_ttl_seconds=60)
    operator._state = cast(ValueState[str], state)
    return operator


def _process_context(timers: FakeTimerService) -> KeyedProcessFunction.Context:
    return cast(KeyedProcessFunction.Context, cast(ProcessContext, FakeProcessContext(timers)))


def _timer_context(timers: FakeTimerService, domain: TimeDomain) -> KeyedProcessFunction.OnTimerContext:
    return cast(
        KeyedProcessFunction.OnTimerContext,
        cast(OnTimerProcessContext, FakeOnTimerContext(timers, domain)),
    )


def _service_contribution(observed_at: int, *, version: str | None = None) -> GraphContribution:
    contributions = contributions_from_servicegraph_datapoint(
        SERVICE_GRAPH_REQUEST_TOTAL,
        {
            "client": "frontend",
            "server": "checkout-api",
            **({"client_service.version": version} if version is not None else {}),
        },
        1,
        observed_at,
    )
    return next(
        item
        for item in contributions
        if isinstance(item.element, GraphNode) and item.element.id == "service:frontend"
    )
