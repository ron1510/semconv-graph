"""PyFlink 2.2.1 wiring for the servicegraph graph-element engine."""

# PyFlink's Python stubs erase DataStream element types and incorrectly declare
# generator-based function overrides as returning None. Keep that unsoundness
# contained in this adapter; domain and wire models remain strict.
# pyright: reportUnknownMemberType=false, reportUnknownLambdaType=false
# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false
# pyright: reportUnknownParameterType=false, reportIncompatibleMethodOverride=false

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Protocol, cast

from pyflink.common import Configuration, Duration, Row, Types, WatermarkStrategy
from pyflink.common.serialization import SerializationSchema, SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import FlatMapFunction, KeyedProcessFunction, RuntimeContext, TimeDomain
from pyflink.datastream.state import ValueState, ValueStateDescriptor
from pyflink.java_gateway import get_gateway

from otel_servicegraph_diff.config import GraphEngineConfig, graph_engine_config_from_env
from otel_servicegraph_diff.engine.elements import (
    GraphContribution,
    GraphContributionRetraction,
    GraphElementEvent,
    GraphElementMutation,
    GraphElementState,
    apply_contribution,
    expire_contributors,
    retract_contribution,
)
from otel_servicegraph_diff.ingest.contributions import iter_otlp_json_contributions
from otel_servicegraph_diff.ingest.entity_events import (
    EntityEventObservation,
    EntitySourceState,
    iter_otlp_json_entity_events,
    reconcile_entity_event,
)
from otel_servicegraph_diff.ingest.metrics import IngestRejection

OUTPUT_ROW_TYPE = Types.ROW_NAMED(["key", "value"], [Types.STRING(), Types.STRING()])
LOGGER = logging.getLogger(__name__)


class TimerService(Protocol):
    def current_processing_time(self) -> int: ...
    def current_watermark(self) -> int: ...
    def register_processing_time_timer(self, timestamp: int) -> None: ...
    def register_event_time_timer(self, timestamp: int) -> None: ...


class ProcessContext(Protocol):
    def timer_service(self) -> TimerService: ...


class OnTimerProcessContext(ProcessContext, Protocol):
    def time_domain(self) -> TimeDomain: ...


class Counter(Protocol):
    def inc(self, n: int = 1) -> None: ...


def main() -> int:
    run_flink_job(graph_engine_config_from_env())
    return 0


def run_flink_job(config: GraphEngineConfig) -> None:
    flink_config = Configuration()
    flink_config.set_string("restart-strategy.type", "fixed-delay")
    flink_config.set_integer("restart-strategy.fixed-delay.attempts", config.restart_attempts)
    flink_config.set_string(
        "restart-strategy.fixed-delay.delay",
        f"{config.restart_delay_seconds} s",
    )
    env = StreamExecutionEnvironment.get_execution_environment(flink_config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(config.parallelism)
    env.enable_checkpointing(config.checkpoint_interval_ms)

    source = _kafka_source(config)
    entity_source = (
        _kafka_source(
            config,
            topic=config.entity_input_topic,
            group_id=config.entity_group_id,
        )
        if config.entity_input_topic is not None
        else None
    )
    event_sink = _kafka_sink(config, config.output_topic)

    _configure_job_graph(env, config, source, event_sink, entity_source)
    env.execute("servicegraph-graph-element-engine")


def _configure_job_graph(
    env: StreamExecutionEnvironment,
    config: GraphEngineConfig,
    source: KafkaSource,
    event_sink: KafkaSink,
    entity_source: KafkaSource | None = None,
) -> None:
    payloads = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "servicegraph-otlp-json")
        .name("servicegraph-kafka-source")
        .uid("graph-v3-kafka-source")
    )
    metric_contributions = (
        payloads.flat_map(_PayloadParser())
        .name("extract-graph-contributions")
        .uid("graph-v3-extract-contributions")
    )
    contributions = metric_contributions
    if entity_source is not None:
        entity_observations = (
            env.from_source(
                entity_source,
                WatermarkStrategy.no_watermarks(),
                "otel-entity-events-json",
            )
            .name("otel-entity-events-kafka-source")
            .uid("graph-v3-entity-events-kafka-source")
            .flat_map(_EntityPayloadParser(config.entity_report_interval_grace_seconds))
            .name("parse-otel-entity-events")
            .uid("graph-v3-parse-otel-entity-events")
        )
        entity_contributions = (
            entity_observations.key_by(_entity_source_key, key_type=Types.STRING())
            .process(_EntityEventContributionsProcess())
            .name("reconcile-otel-entity-events")
            .uid("graph-v3-reconcile-otel-entity-events")
        )
        contributions = metric_contributions.union(entity_contributions)

    timestamped_contributions = (
        contributions.assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(
                Duration.of_seconds(config.allowed_lateness_seconds)
            )
            .with_idleness(Duration.of_seconds(max(config.allowed_lateness_seconds * 2, 1)))
            .with_timestamp_assigner(_ObservationTimestampAssigner())
        )
        .name("graph-contribution-watermarks")
        .uid("graph-v3-watermarks")
    )
    events = (
        timestamped_contributions.key_by(_element_key, key_type=Types.STRING())
        .process(
            _GraphElementLifecycleProcess(config.contributor_ttl_seconds)
        )
        .name("graph-element-lifecycle")
        .uid("graph-v3-element-lifecycle")
    )
    event_rows = (
        events.map(_event_row, output_type=OUTPUT_ROW_TYPE)
        .name("serialize-graph-element-events")
        .uid("graph-v3-serialize-events")
    )
    event_rows.sink_to(event_sink).name("graph-element-events").uid("graph-v3-events-sink")


def _kafka_source(
    config: GraphEngineConfig,
    *,
    topic: str | None = None,
    group_id: str | None = None,
) -> KafkaSource:
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(topic or config.input_topic)
        .set_group_id(group_id or config.group_id)
        .set_starting_offsets(
            KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
        )
        .set_property("commit.offsets.on.checkpoint", "true")
        .set_property("allow.auto.create.topics", "false")
    )
    for key, value in config.kafka_client_properties.items():
        builder.set_property(key, value)
    return builder.set_value_only_deserializer(SimpleStringSchema()).build()


def _kafka_sink(config: GraphEngineConfig, topic: str) -> KafkaSink:
    jvm = get_gateway().jvm
    # Py4J resolves checked-in Java classes dynamically and exposes no callable
    # type metadata. Keep that unsound boundary to these two constructor calls.
    key_serializer = SerializationSchema(
        jvm.io.extendedotel.flink.FirstColumnStringSerializationSchema()  # pyright: ignore[reportCallIssue, reportOptionalCall, reportAttributeAccessIssue, reportOptionalMemberAccess]
    )
    value_serializer = SerializationSchema(
        jvm.io.extendedotel.flink.SecondColumnStringSerializationSchema()  # pyright: ignore[reportCallIssue, reportOptionalCall, reportAttributeAccessIssue, reportOptionalMemberAccess]
    )
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_key_serialization_schema(key_serializer)
        .set_value_serialization_schema(value_serializer)
        .build()
    )
    builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_record_serializer(serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    )
    builder.set_property("allow.auto.create.topics", "false")
    for key, value in config.kafka_client_properties.items():
        builder.set_property(key, value)
    return builder.build()


class _PayloadParser(FlatMapFunction):
    def __init__(self) -> None:
        self._rejected_inputs: Counter | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._rejected_inputs = cast(Counter, runtime_context.get_metrics_group().counter("rejected_inputs"))

    def flat_map(self, value: str) -> Iterable[GraphContribution]:
        for parsed in iter_otlp_json_contributions(value):
            match parsed:
                case IngestRejection():
                    self._require_rejected_inputs().inc()
                    LOGGER.warning(
                        "discarding rejected servicegraph input: reason=%s detail=%s",
                        parsed.reason,
                        (parsed.detail or "")[:512],
                    )
                case GraphContribution():
                    yield parsed

    def _require_rejected_inputs(self) -> Counter:
        if self._rejected_inputs is None:
            raise RuntimeError("parser metrics accessed before operator initialization")
        return self._rejected_inputs


class _EntityPayloadParser(FlatMapFunction):
    def __init__(self, report_interval_grace_seconds: int) -> None:
        self._report_interval_grace_seconds = report_interval_grace_seconds
        self._rejected_inputs: Counter | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._rejected_inputs = cast(Counter, runtime_context.get_metrics_group().counter("rejected_entity_events"))

    def flat_map(self, value: str) -> Iterable[EntityEventObservation]:
        for parsed in iter_otlp_json_entity_events(
            value,
            report_interval_grace_seconds=self._report_interval_grace_seconds,
        ):
            match parsed:
                case IngestRejection():
                    self._require_rejected_inputs().inc()
                    LOGGER.warning(
                        "discarding rejected OTel entity event: reason=%s detail=%s",
                        parsed.reason,
                        (parsed.detail or "")[:512],
                    )
                case _:
                    yield parsed

    def _require_rejected_inputs(self) -> Counter:
        if self._rejected_inputs is None:
            raise RuntimeError("entity parser metrics accessed before operator initialization")
        return self._rejected_inputs


class _EntityEventContributionsProcess(KeyedProcessFunction):
    def __init__(self) -> None:
        self._state: ValueState[str] | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        descriptor = ValueStateDescriptor("otel-entity-source-state-v1", Types.STRING())
        self._state = runtime_context.get_state(descriptor)

    def process_element(
        self,
        value: EntityEventObservation,
        ctx: KeyedProcessFunction.Context,
    ) -> Iterable[GraphElementMutation]:
        del ctx
        state_handle = self._require_state()
        previous_json = state_handle.value()
        previous = EntitySourceState.model_validate_json(previous_json) if previous_json else None
        result = reconcile_entity_event(previous, value)
        if result.state is not previous:
            state_handle.update(result.state.model_dump_json())
        return result.mutations

    def _require_state(self) -> ValueState[str]:
        if self._state is None:
            raise RuntimeError("entity source state accessed before operator initialization")
        return self._state


class _GraphElementLifecycleProcess(KeyedProcessFunction):
    def __init__(self, ttl_seconds: int) -> None:
        self._ttl_seconds = ttl_seconds
        self._state: ValueState[str] | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        descriptor = ValueStateDescriptor("graph-element-lifecycle-state-v3", Types.STRING())
        self._state = runtime_context.get_state(descriptor)

    def process_element(
        self,
        value: GraphElementMutation,
        ctx: KeyedProcessFunction.Context,
    ) -> Iterable[GraphElementEvent]:
        state_handle = self._require_state()
        previous_json = state_handle.value()
        previous = GraphElementState.model_validate_json(previous_json) if previous_json else None
        timer_service = cast(ProcessContext, ctx).timer_service()
        watermark_nano = max(timer_service.current_watermark(), 0) * 1_000_000
        processing_time = timer_service.current_processing_time()
        match value:
            case GraphContribution():
                result = apply_contribution(
                    previous,
                    value,
                    ttl_seconds=self._ttl_seconds,
                    event_expiry_base_unix_nano=watermark_nano,
                    processing_time_unix_ms=processing_time,
                )
            case GraphContributionRetraction():
                result = retract_contribution(previous, value)
        if result.state is previous:
            return ()
        if result.state is None:
            state_handle.clear()
            return (result.event,) if result.event is not None else ()
        state_handle.update(result.state.model_dump_json())
        if isinstance(value, GraphContribution):
            snapshot = result.state.contributors[value.contributor_id]
            if snapshot.event_expires_at_unix_nano is not None:
                timer_service.register_event_time_timer(_timer_millis(snapshot.event_expires_at_unix_nano))
            if snapshot.processing_expires_at_unix_ms is not None:
                timer_service.register_processing_time_timer(snapshot.processing_expires_at_unix_ms)
        return (result.event,) if result.event is not None else ()

    def on_timer(
        self,
        timestamp: int,
        ctx: KeyedProcessFunction.OnTimerContext,
    ) -> Iterable[GraphElementEvent]:
        state_handle = self._require_state()
        state_json = state_handle.value()
        if not state_json:
            return ()
        state = GraphElementState.model_validate_json(state_json)
        time_domain = cast(OnTimerProcessContext, ctx).time_domain()
        if time_domain is TimeDomain.PROCESSING_TIME:
            result = expire_contributors(state, clock="processing_time", timestamp=timestamp)
        else:
            result = expire_contributors(state, clock="event_time", timestamp=timestamp * 1_000_000)
        if result.state is None:
            state_handle.clear()
        elif result.state != state:
            state_handle.update(result.state.model_dump_json())
        return (result.event,) if result.event is not None else ()

    def _require_state(self) -> ValueState[str]:
        if self._state is None:
            raise RuntimeError("graph element state accessed before operator initialization")
        return self._state


class _ObservationTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: GraphElementMutation, record_timestamp: int) -> int:
        del record_timestamp
        return value.observed_at_unix_nano // 1_000_000


def _element_key(item: GraphElementMutation) -> str:
    match item:
        case GraphContribution():
            return item.element.id
        case GraphContributionRetraction():
            return item.element_id


def _entity_source_key(item: EntityEventObservation) -> str:
    return item.source_key


def _event_row(event: GraphElementEvent) -> Row:
    return Row(event.element_id, event.model_dump_json())


def _timer_millis(unix_nano: int) -> int:
    return (unix_nano + 999_999) // 1_000_000


if __name__ == "__main__":
    raise SystemExit(main())
