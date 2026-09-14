# Framework operators are intentionally exercised through their private state seams.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import logging
from typing import cast

import pytest

pytest.importorskip("pyflink")

from pyflink.datastream.functions import KeyedProcessFunction, TimeDomain
from pyflink.datastream.state import MapState, ValueState

from otel_servicegraph_diff.engine.elements import (
    ContributorSnapshot,
    GraphContribution,
    GraphContributionRetraction,
    GraphElementAggregateState,
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
        self.updates: list[T] = []
        self.clear_count = 0

    def value(self) -> T | None:
        return self.serialized

    def update(self, value: T) -> None:
        self.serialized = value
        self.updates.append(value)

    def clear(self) -> None:
        self.serialized = None
        self.clear_count += 1


class FakeMapState[K, V]:
    def __init__(self) -> None:
        self.entries: dict[K, V] = {}
        self.puts: list[tuple[K, V]] = []
        self.removals: list[K] = []
        self.clear_count = 0

    def items(self) -> list[tuple[K, V]]:
        return list(self.entries.items())

    def put(self, key: K, value: V) -> None:
        self.entries[key] = value
        self.puts.append((key, value))

    def remove(self, key: K) -> None:
        self.entries.pop(key, None)
        self.removals.append(key)

    def clear(self) -> None:
        self.entries.clear()
        self.clear_count += 1


class FakeLifecycleState:
    def __init__(self) -> None:
        self.contributors = FakeMapState[str, str]()
        self.aggregate = FakeValueState[str]()
        self.event_timer = FakeValueState[int]()
        self.processing_timer = FakeValueState[int]()


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
        self.deleted_event: list[int] = []
        self.deleted_processing: list[int] = []
        self.active_event: set[int] = set()
        self.active_processing: set[int] = set()

    def current_processing_time(self) -> int:
        return self.processing_time

    def current_watermark(self) -> int:
        return self.watermark

    def register_event_time_timer(self, timestamp: int) -> None:
        self.registered_event.append(timestamp)
        self.active_event.add(timestamp)

    def register_processing_time_timer(self, timestamp: int) -> None:
        self.registered_processing.append(timestamp)
        self.active_processing.add(timestamp)

    def delete_event_time_timer(self, timestamp: int) -> None:
        self.deleted_event.append(timestamp)
        self.active_event.discard(timestamp)

    def delete_processing_time_timer(self, timestamp: int) -> None:
        self.deleted_processing.append(timestamp)
        self.active_processing.discard(timestamp)


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
    state = FakeLifecycleState()
    timers = FakeTimerService()
    operator = _operator(state)
    contribution = _service_contribution(1_000_000_001)

    events = tuple(operator.process_element(contribution, _process_context(timers)))

    assert len(events) == 1
    assert isinstance(events[0], GraphElementUpsertEvent)
    assert timers.registered_event == [7_000]
    assert timers.registered_processing == [15_000]
    persisted = _persisted_state(state)
    assert persisted.element_id == contribution.element.id
    assert set(persisted.contributors) == {contribution.contributor_id}


def test_refresh_registers_new_timers_and_old_timer_is_ignored() -> None:
    state = FakeLifecycleState()
    timers = FakeTimerService()
    operator = _operator(state)
    context = _process_context(timers)
    tuple(operator.process_element(_service_contribution(1_000_000_001, version="1"), context))

    timers.processing_time = 11_000
    tuple(operator.process_element(_service_contribution(2_000_000_001, version="2"), context))
    stale = tuple(operator.on_timer(7_000, _timer_context(timers, TimeDomain.EVENT_TIME)))

    assert timers.registered_event == [7_000, 8_000]
    assert timers.registered_processing == [15_000, 16_000]
    assert timers.deleted_event == [7_000]
    assert timers.deleted_processing == [15_000]
    assert stale == ()
    persisted = _persisted_state(state)
    snapshot = next(iter(persisted.contributors.values()))
    assert snapshot.observed_at_unix_nano == 2_000_000_001


def test_event_timer_deletes_final_contributor_and_clears_state() -> None:
    state = FakeLifecycleState()
    timers = FakeTimerService()
    operator = _operator(state)
    tuple(operator.process_element(_service_contribution(1_000_000_001), _process_context(timers)))

    events = tuple(operator.on_timer(7_000, _timer_context(timers, TimeDomain.EVENT_TIME)))

    assert len(events) == 1
    assert events[0].operation == "delete"
    assert state.contributors.entries == {}
    assert state.aggregate.serialized is None
    assert state.event_timer.serialized is None
    assert state.processing_timer.serialized is None


def test_explicit_retraction_deletes_final_contributor_and_clears_state() -> None:
    state = FakeLifecycleState()
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
    assert state.contributors.entries == {}
    assert state.aggregate.serialized is None


def test_non_expiring_contribution_registers_no_timers_and_survives_timer_callbacks() -> None:
    state = FakeLifecycleState()
    timers = FakeTimerService()
    operator = _operator(state)
    contribution = _service_contribution(1_000_000_001).model_copy(update={"ttl_seconds": 0})

    events = tuple(operator.process_element(contribution, _process_context(timers)))

    assert len(events) == 1
    assert timers.registered_event == []
    assert timers.registered_processing == []
    assert tuple(operator.on_timer(10**12, _timer_context(timers, TimeDomain.EVENT_TIME))) == ()
    assert tuple(operator.on_timer(10**12, _timer_context(timers, TimeDomain.PROCESSING_TIME))) == ()
    assert _persisted_state(state).contributors


def test_entity_source_operator_checkpoints_snapshot_reconciliation() -> None:
    state = FakeValueState[str]()
    operator = _EntityEventContributionsProcess()
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
    state = FakeLifecycleState()
    timers = FakeTimerService(processing_time=20_000)
    operator = _operator(state)
    tuple(operator.process_element(_service_contribution(1_000_000_001), _process_context(timers)))

    events = tuple(operator.on_timer(25_000, _timer_context(timers, TimeDomain.PROCESSING_TIME)))

    assert len(events) == 1
    assert events[0].operation == "delete"
    assert state.contributors.entries == {}
    assert state.aggregate.serialized is None


def test_watermark_extends_event_expiry_without_changing_processing_expiry() -> None:
    state = FakeLifecycleState()
    timers = FakeTimerService(watermark=4_000, processing_time=10_000)
    operator = _operator(state)

    tuple(operator.process_element(_service_contribution(1_000_000_001), _process_context(timers)))

    assert timers.registered_event == [9_000]
    assert timers.registered_processing == [15_000]


def test_partial_expiry_replaces_both_element_timers() -> None:
    state = FakeLifecycleState()
    timers = FakeTimerService(processing_time=10_000)
    operator = _operator(state)
    first = _service_contribution(1_000_000_001).model_copy(update={"contributor_id": "first"})
    second = _service_contribution(2_000_000_001).model_copy(update={"contributor_id": "second"})
    tuple(operator.process_element(first, _process_context(timers)))
    timers.processing_time = 11_000
    tuple(operator.process_element(second, _process_context(timers)))

    events = tuple(operator.on_timer(15_000, _timer_context(timers, TimeDomain.PROCESSING_TIME)))

    assert events == ()
    assert set(_persisted_state(state).contributors) == {"second"}
    assert timers.active_event == {8_000}
    assert timers.active_processing == {16_000}


def test_granular_state_updates_one_of_two_thousand_contributors() -> None:
    state = FakeLifecycleState()
    timers = FakeTimerService()
    operator = _operator(state)
    node = GraphNode(id="service:frontend", type="service", attributes={"service.name": "frontend"})
    snapshots = {
        f"contributor-{index}": ContributorSnapshot(
            observed_at_unix_nano=index + 1,
            event_expires_at_unix_nano=10_000_000_000 + index,
            processing_expires_at_unix_ms=20_000 + index,
            element=node,
        )
        for index in range(2_000)
    }
    previous = GraphElementState(
        element_id=node.id,
        contributors=snapshots,
        last_payload_hash="payload-hash",
    )
    operator._persist_state(None, previous)
    state.contributors.puts.clear()
    changed = snapshots["contributor-1000"].model_copy(update={"observed_at_unix_nano": 50_000})
    current = previous.model_copy(
        update={"contributors": {**snapshots, "contributor-1000": changed}}
    )

    operator._persist_state(previous, current)
    operator._schedule_timers(current, timers)

    assert [key for key, _ in state.contributors.puts] == ["contributor-1000"]
    assert len(state.aggregate.updates) == 1
    assert len(timers.active_event) == 1
    assert len(timers.active_processing) == 1


def test_dense_contributor_deadlines_share_timer_buckets() -> None:
    state = FakeLifecycleState()
    timers = FakeTimerService()
    operator = _operator(state)
    node = GraphNode(id="service:frontend", type="service")
    snapshots = {
        f"contributor-{index}": ContributorSnapshot(
            observed_at_unix_nano=index + 1,
            event_expires_at_unix_nano=10_000_000_001 + index * 1_000_000,
            processing_expires_at_unix_ms=20_001 + index,
            element=node,
        )
        for index in range(500)
    }
    current = GraphElementState(
        element_id=node.id,
        contributors=snapshots,
        last_payload_hash="payload-hash",
    )

    operator._persist_state(None, current)
    operator._schedule_timers(current, timers)

    assert timers.active_event == {11_000}
    assert timers.active_processing == {21_000}


def test_empty_state_timer_emits_nothing() -> None:
    operator = _operator(FakeLifecycleState())

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
    assert "discarding rejected graph metric input" in caplog.text
    assert "{not-json" not in caplog.text


def test_operator_requires_runtime_initialization() -> None:
    operator = _GraphElementLifecycleProcess(ttl_seconds=5)

    with pytest.raises(RuntimeError, match="graph element state accessed before operator initialization"):
        operator._require_state()


def _operator(state: FakeLifecycleState) -> _GraphElementLifecycleProcess:
    operator = _GraphElementLifecycleProcess(ttl_seconds=5)
    operator._contributors = cast(MapState[str, str], state.contributors)
    operator._aggregate = cast(ValueState[str], state.aggregate)
    operator._next_event_timer = cast(ValueState[int], state.event_timer)
    operator._next_processing_timer = cast(ValueState[int], state.processing_timer)
    return operator


def _persisted_state(state: FakeLifecycleState) -> GraphElementState:
    assert state.aggregate.serialized is not None
    aggregate = GraphElementAggregateState.model_validate_json(state.aggregate.serialized)
    return GraphElementState(
        element_id=aggregate.element_id,
        contributors={
            contributor_id: ContributorSnapshot.model_validate_json(snapshot)
            for contributor_id, snapshot in state.contributors.entries.items()
        },
        metrics=aggregate.metrics,
        last_payload_hash=aggregate.last_payload_hash,
    )


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
