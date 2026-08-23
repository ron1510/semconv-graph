# PyFlink erases generic stream types; these tests intentionally mock that boundary.
# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false
# pyright: reportUnknownLambdaType=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import ANY, MagicMock, call

import pytest

pytest.importorskip("pyflink")

from pyflink.datastream import RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import DeliveryGuarantee, KafkaOffsetResetStrategy

from otel_servicegraph_diff import flink_job
from otel_servicegraph_diff.config import GraphEngineConfig
from otel_servicegraph_diff.engine.elements import GraphContribution, GraphNode, apply_contribution
from otel_servicegraph_diff.ingest.contributions import contributions_from_servicegraph_datapoint
from otel_servicegraph_diff.ingest.metrics import SERVICE_GRAPH_REQUEST_TOTAL


def test_main_loads_settings_and_runs_job(monkeypatch: pytest.MonkeyPatch) -> None:
    config = GraphEngineConfig()
    load_config = MagicMock(return_value=config)
    run = MagicMock()
    monkeypatch.setattr(flink_job, "graph_engine_config_from_env", load_config)
    monkeypatch.setattr(flink_job, "run_flink_job", run)

    assert flink_job.main() == 0
    load_config.assert_called_once_with()
    run.assert_called_once_with(config)


def test_run_flink_job_configures_runtime_and_executes_once(monkeypatch: pytest.MonkeyPatch) -> None:
    config = GraphEngineConfig(
        parallelism=2,
        checkpoint_interval_ms=5_000,
        restart_attempts=4,
        restart_delay_seconds=7,
    )
    flink_config = _FakeConfiguration()
    env = MagicMock()
    source = object()
    sink = object()
    configure_graph = MagicMock()
    environment_factory = MagicMock(return_value=env)

    monkeypatch.setattr(flink_job, "Configuration", lambda: flink_config)
    monkeypatch.setattr(
        flink_job,
        "StreamExecutionEnvironment",
        SimpleNamespace(get_execution_environment=environment_factory),
    )
    monkeypatch.setattr(flink_job, "_kafka_source", MagicMock(return_value=source))
    monkeypatch.setattr(flink_job, "_kafka_sink", MagicMock(return_value=sink))
    monkeypatch.setattr(flink_job, "_configure_job_graph", configure_graph)

    flink_job.run_flink_job(config)

    assert flink_config.values == {
        "restart-strategy.type": "fixed-delay",
        "restart-strategy.fixed-delay.attempts": 4,
        "restart-strategy.fixed-delay.delay": "7 s",
    }
    environment_factory.assert_called_once_with(flink_config)
    env.set_runtime_mode.assert_called_once_with(RuntimeExecutionMode.STREAMING)
    env.set_parallelism.assert_called_once_with(2)
    env.enable_checkpointing.assert_called_once_with(5_000)
    flink_job._kafka_source.assert_called_once_with(config)  # pyright: ignore[reportFunctionMemberAccess]
    flink_job._kafka_sink.assert_called_once_with(config, config.output_topic)  # pyright: ignore[reportFunctionMemberAccess]
    configure_graph.assert_called_once_with(env, config, source, sink, None)
    env.execute.assert_called_once_with("servicegraph-graph-element-engine")


def test_run_flink_job_builds_independent_optional_entity_source(monkeypatch: pytest.MonkeyPatch) -> None:
    config = GraphEngineConfig(entity_input_topic="otel.entity.events")
    env = MagicMock()
    metrics_source = object()
    entity_source = object()
    sink = object()
    source_builder = MagicMock(side_effect=[metrics_source, entity_source])
    configure_graph = MagicMock()

    monkeypatch.setattr(flink_job, "Configuration", _FakeConfiguration)
    monkeypatch.setattr(
        flink_job,
        "StreamExecutionEnvironment",
        SimpleNamespace(get_execution_environment=MagicMock(return_value=env)),
    )
    monkeypatch.setattr(flink_job, "_kafka_source", source_builder)
    monkeypatch.setattr(flink_job, "_kafka_sink", MagicMock(return_value=sink))
    monkeypatch.setattr(flink_job, "_configure_job_graph", configure_graph)

    flink_job.run_flink_job(config)

    assert source_builder.call_args_list == [
        call(config),
        call(
            config,
            topic="otel.entity.events",
            group_id="graph-element-engine-entities",
        ),
    ]
    configure_graph.assert_called_once_with(env, config, metrics_source, sink, entity_source)


def test_configure_job_graph_builds_named_stable_operator_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    env = MagicMock()
    source = object()
    sink = object()
    watermark = MagicMock()
    watermark.with_idleness.return_value = watermark
    watermark.with_timestamp_assigner.return_value = watermark
    strategy = SimpleNamespace(
        no_watermarks=MagicMock(return_value="no-watermarks"),
        for_bounded_out_of_orderness=MagicMock(return_value=watermark),
    )
    duration = SimpleNamespace(of_seconds=MagicMock(side_effect=_duration))
    monkeypatch.setattr(flink_job, "WatermarkStrategy", strategy)
    monkeypatch.setattr(flink_job, "Duration", duration)
    monkeypatch.setattr(flink_job, "Types", SimpleNamespace(STRING=MagicMock(return_value="string")))

    config = GraphEngineConfig(allowed_lateness_seconds=3, contributor_ttl_seconds=10)
    flink_job._configure_job_graph(
        cast(Any, env),
        config,
        cast(Any, source),
        cast(Any, sink),
    )

    source_operator = env.from_source.return_value
    source_named = source_operator.name.return_value
    payloads = source_named.uid.return_value
    parser_operator = payloads.flat_map.return_value
    parser_named = parser_operator.name.return_value
    parser_uid = parser_named.uid.return_value
    watermark_operator = parser_uid.assign_timestamps_and_watermarks.return_value
    watermark_named = watermark_operator.name.return_value
    contributions = watermark_named.uid.return_value
    keyed = contributions.key_by.return_value
    process_operator = keyed.process.return_value
    process_named = process_operator.name.return_value
    events = process_named.uid.return_value
    map_operator = events.map.return_value
    map_named = map_operator.name.return_value
    event_rows = map_named.uid.return_value
    sink_operator = event_rows.sink_to.return_value
    sink_named = sink_operator.name.return_value

    env.from_source.assert_called_once_with(source, "no-watermarks", "servicegraph-otlp-json")
    source_operator.name.assert_called_once_with("servicegraph-kafka-source")
    source_named.uid.assert_called_once_with("graph-v3-kafka-source")
    payloads.flat_map.assert_called_once_with(ANY)
    parser_operator.name.assert_called_once_with("extract-graph-contributions")
    parser_named.uid.assert_called_once_with("graph-v3-extract-contributions")
    strategy.for_bounded_out_of_orderness.assert_called_once_with("3s")
    watermark.with_idleness.assert_called_once_with("6s")
    watermark.with_timestamp_assigner.assert_called_once_with(ANY)
    parser_uid.assign_timestamps_and_watermarks.assert_called_once_with(watermark)
    watermark_operator.name.assert_called_once_with("graph-contribution-watermarks")
    watermark_named.uid.assert_called_once_with("graph-v3-watermarks")
    contributions.key_by.assert_called_once_with(flink_job._element_key, key_type="string")
    keyed.process.assert_called_once_with(ANY)
    process_operator.name.assert_called_once_with("graph-element-lifecycle")
    process_named.uid.assert_called_once_with("graph-v3-element-lifecycle")
    events.map.assert_called_once_with(flink_job._event_row, output_type=flink_job.OUTPUT_ROW_TYPE)
    map_operator.name.assert_called_once_with("serialize-graph-element-events")
    map_named.uid.assert_called_once_with("graph-v3-serialize-events")
    event_rows.sink_to.assert_called_once_with(sink)
    sink_operator.name.assert_called_once_with("graph-element-events")
    sink_named.uid.assert_called_once_with("graph-v3-events-sink")


def test_configure_job_graph_unions_checkpointed_entity_reconciliation(monkeypatch: pytest.MonkeyPatch) -> None:
    env = MagicMock()
    metric_source = object()
    entity_source = object()
    sink = object()
    metric_source_operator = MagicMock()
    entity_source_operator = MagicMock()
    env.from_source.side_effect = [metric_source_operator, entity_source_operator]
    metric_payloads = metric_source_operator.name.return_value.uid.return_value
    metric_contributions = metric_payloads.flat_map.return_value.name.return_value.uid.return_value
    entity_payloads = entity_source_operator.name.return_value.uid.return_value
    entity_observations = entity_payloads.flat_map.return_value.name.return_value.uid.return_value
    entity_mutations = entity_observations.key_by.return_value.process.return_value.name.return_value.uid.return_value
    merged = metric_contributions.union.return_value
    timestamped = merged.assign_timestamps_and_watermarks.return_value.name.return_value.uid.return_value
    events = timestamped.key_by.return_value.process.return_value.name.return_value.uid.return_value
    rows = events.map.return_value.name.return_value.uid.return_value
    rows.sink_to.return_value.name.return_value.uid.return_value = None
    watermark = MagicMock()
    watermark.with_idleness.return_value = watermark
    watermark.with_timestamp_assigner.return_value = watermark
    monkeypatch.setattr(
        flink_job,
        "WatermarkStrategy",
        SimpleNamespace(
            no_watermarks=MagicMock(return_value="no-watermarks"),
            for_bounded_out_of_orderness=MagicMock(return_value=watermark),
        ),
    )
    monkeypatch.setattr(flink_job, "Duration", SimpleNamespace(of_seconds=MagicMock(side_effect=_duration)))
    monkeypatch.setattr(flink_job, "Types", SimpleNamespace(STRING=MagicMock(return_value="string")))
    config = GraphEngineConfig(
        entity_input_topic="otel.entity.events",
        allowed_lateness_seconds=3,
        contributor_ttl_seconds=10,
    )

    flink_job._configure_job_graph(
        cast(Any, env),
        config,
        cast(Any, metric_source),
        cast(Any, sink),
        cast(Any, entity_source),
    )

    assert env.from_source.call_args_list == [
        call(metric_source, "no-watermarks", "servicegraph-otlp-json"),
        call(entity_source, "no-watermarks", "otel-entity-events-json"),
    ]
    entity_source_operator.name.assert_called_once_with("otel-entity-events-kafka-source")
    entity_source_operator.name.return_value.uid.assert_called_once_with("graph-v3-entity-events-kafka-source")
    entity_observations.key_by.assert_called_once_with(flink_job._entity_source_key, key_type="string")
    entity_observations.key_by.return_value.process.return_value.name.assert_called_once_with(
        "reconcile-otel-entity-events"
    )
    entity_observations.key_by.return_value.process.return_value.name.return_value.uid.assert_called_once_with(
        "graph-v3-reconcile-otel-entity-events"
    )
    metric_contributions.union.assert_called_once_with(entity_mutations)


def test_kafka_source_maps_all_contract_properties(monkeypatch: pytest.MonkeyPatch) -> None:
    builder = _fluent_builder(
        "set_bootstrap_servers",
        "set_topics",
        "set_group_id",
        "set_starting_offsets",
        "set_property",
        "set_value_only_deserializer",
    )
    source = object()
    builder.build.return_value = source
    monkeypatch.setattr(flink_job, "KafkaSource", SimpleNamespace(builder=MagicMock(return_value=builder)))
    monkeypatch.setattr(
        flink_job,
        "KafkaOffsetsInitializer",
        SimpleNamespace(committed_offsets=MagicMock(return_value="committed-earliest")),
    )
    deserializer = object()
    monkeypatch.setattr(flink_job, "SimpleStringSchema", MagicMock(return_value=deserializer))
    config = GraphEngineConfig(kafka_bootstrap_servers="one:9092,two:9092")

    assert flink_job._kafka_source(config) is source
    builder.set_bootstrap_servers.assert_called_once_with("one:9092,two:9092")
    builder.set_topics.assert_called_once_with(config.input_topic)
    builder.set_group_id.assert_called_once_with(config.group_id)
    flink_job.KafkaOffsetsInitializer.committed_offsets.assert_called_once_with(  # pyright: ignore[reportFunctionMemberAccess]
        KafkaOffsetResetStrategy.EARLIEST
    )
    builder.set_starting_offsets.assert_called_once_with("committed-earliest")
    builder.set_property.assert_has_calls(
        [
            call("commit.offsets.on.checkpoint", "true"),
            call("allow.auto.create.topics", "false"),
            call("security.protocol", "PLAINTEXT"),
        ]
    )
    builder.set_value_only_deserializer.assert_called_once_with(deserializer)
    builder.build.assert_called_once_with()


def test_kafka_sink_maps_serializers_delivery_and_properties(monkeypatch: pytest.MonkeyPatch) -> None:
    key_java = object()
    value_java = object()
    jvm = SimpleNamespace(
        io=SimpleNamespace(
            extendedotel=SimpleNamespace(
                flink=SimpleNamespace(
                    FirstColumnStringSerializationSchema=MagicMock(return_value=key_java),
                    SecondColumnStringSerializationSchema=MagicMock(return_value=value_java),
                )
            )
        )
    )
    monkeypatch.setattr(flink_job, "get_gateway", MagicMock(return_value=SimpleNamespace(jvm=jvm)))
    serialization_schema = MagicMock(side_effect=_schema)
    monkeypatch.setattr(flink_job, "SerializationSchema", serialization_schema)

    record_builder = _fluent_builder("set_topic", "set_key_serialization_schema", "set_value_serialization_schema")
    record_serializer = object()
    record_builder.build.return_value = record_serializer
    monkeypatch.setattr(
        flink_job,
        "KafkaRecordSerializationSchema",
        SimpleNamespace(builder=MagicMock(return_value=record_builder)),
    )
    sink_builder = _fluent_builder(
        "set_bootstrap_servers",
        "set_record_serializer",
        "set_delivery_guarantee",
        "set_property",
    )
    sink = object()
    sink_builder.build.return_value = sink
    monkeypatch.setattr(flink_job, "KafkaSink", SimpleNamespace(builder=MagicMock(return_value=sink_builder)))
    config = GraphEngineConfig(kafka_bootstrap_servers="broker:9092")

    assert flink_job._kafka_sink(config, "events") is sink
    serialization_schema.assert_has_calls([call(key_java), call(value_java)])
    record_builder.set_topic.assert_called_once_with("events")
    record_builder.set_key_serialization_schema.assert_called_once_with(("schema", key_java))
    record_builder.set_value_serialization_schema.assert_called_once_with(("schema", value_java))
    sink_builder.set_bootstrap_servers.assert_called_once_with("broker:9092")
    sink_builder.set_record_serializer.assert_called_once_with(record_serializer)
    sink_builder.set_delivery_guarantee.assert_called_once_with(DeliveryGuarantee.AT_LEAST_ONCE)
    sink_builder.set_property.assert_has_calls(
        [call("allow.auto.create.topics", "false"), call("security.protocol", "PLAINTEXT")]
    )
    sink_builder.build.assert_called_once_with()


def test_payload_parser_initializes_metric_and_yields_direct_contributions() -> None:
    counter = MagicMock()
    metrics = MagicMock()
    metrics.counter.return_value = counter
    runtime = MagicMock()
    runtime.get_metrics_group.return_value = metrics
    parser = flink_job._PayloadParser()

    with pytest.raises(RuntimeError, match="before operator initialization"):
        parser._require_rejected_inputs()

    parser.open(runtime)
    contributions = tuple(parser.flat_map(_valid_metrics_payload()))

    metrics.counter.assert_called_once_with("rejected_inputs")
    assert len(contributions) == 3
    assert len({item.element.id for item in contributions}) == 3
    counter.inc.assert_not_called()


def test_payload_parser_yields_multiple_points_and_ignores_unknown_metrics() -> None:
    payload = json.loads(_valid_metrics_payload())
    metrics = payload["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
    second_point = dict(metrics[0]["sum"]["dataPoints"][0])
    second_point.update({"asInt": "2", "timeUnixNano": "2234567890"})
    metrics[0]["sum"]["dataPoints"].append(second_point)
    metrics.append({"name": "traces_service_graph_request_duration_seconds"})

    counter = MagicMock()
    runtime = MagicMock()
    runtime.get_metrics_group.return_value.counter.return_value = counter
    parser = flink_job._PayloadParser()
    parser.open(runtime)

    contributions = tuple(parser.flat_map(json.dumps(payload)))

    assert [next(iter(item.metric_deltas.values())) for item in contributions if item.metric_deltas] == [1, 2]
    counter.inc.assert_not_called()


def test_process_open_registers_versioned_state_without_framework_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        flink_job,
        "Types",
        SimpleNamespace(STRING=MagicMock(return_value="string")),
    )
    descriptors: list[_FakeDescriptor] = []

    def descriptor(name: str, value_type: object) -> _FakeDescriptor:
        result = _FakeDescriptor(name, value_type)
        descriptors.append(result)
        return result

    monkeypatch.setattr(flink_job, "ValueStateDescriptor", descriptor)
    states = [object(), object()]
    runtime = MagicMock()
    runtime.get_state.side_effect = states
    entity_operator = flink_job._EntityEventContributionsProcess()
    lifecycle_operator = flink_job._GraphElementLifecycleProcess(ttl_seconds=5)

    entity_operator.open(runtime)
    lifecycle_operator.open(runtime)

    assert [(item.name, item.value_type, item.ttl) for item in descriptors] == [
        ("otel-entity-source-state-v1", "string", None),
        ("graph-element-lifecycle-state-v3", "string", None),
    ]
    assert entity_operator._state is states[0]  # pyright: ignore[reportPrivateUsage]
    assert lifecycle_operator._state is states[1]  # pyright: ignore[reportPrivateUsage]


def test_small_stream_adapters_preserve_identity_and_time() -> None:
    parsed = _parsed_contribution()
    assigner = flink_job._ObservationTimestampAssigner()
    assert assigner.extract_timestamp(parsed, record_timestamp=999) == 1_234
    assert flink_job._timer_millis(1_000_000_000) == 1_000
    assert flink_job._timer_millis(1_000_000_001) == 1_001

    contribution = GraphContribution(
        contributor_id="contributor-a",
        observed_at_unix_nano=1_234_567_890,
        element=GraphNode(id="service:frontend", type="service"),
    )
    result = apply_contribution(
        None,
        contribution,
        ttl_seconds=5,
        processing_time_unix_ms=10,
        emitted_at_unix_ms=10,
    )
    assert result.event is not None
    assert flink_job._element_key(contribution) == contribution.element.id
    row = flink_job._event_row(result.event)
    assert row[0] == result.event.element_id
    assert '"operation":"upsert"' in row[1]


class _FakeConfiguration:
    def __init__(self) -> None:
        self.values: dict[str, str | int] = {}

    def set_string(self, key: str, value: str) -> None:
        self.values[key] = value

    def set_integer(self, key: str, value: int) -> None:
        self.values[key] = value


class _FakeDescriptor:
    def __init__(self, name: str, value_type: object) -> None:
        self.name = name
        self.value_type = value_type
        self.ttl: object | None = None

    def enable_time_to_live(self, ttl: object) -> None:
        self.ttl = ttl


def _fluent_builder(*method_names: str) -> MagicMock:
    builder = MagicMock()
    for method_name in method_names:
        getattr(builder, method_name).return_value = builder
    return builder


def _duration(seconds: int) -> str:
    return f"{seconds}s"


def _schema(value: object) -> tuple[str, object]:
    return "schema", value


def _parsed_contribution() -> GraphContribution:
    contributions = contributions_from_servicegraph_datapoint(
        SERVICE_GRAPH_REQUEST_TOTAL,
        {"client": "frontend", "server": "checkout-api"},
        1,
        1_234_567_890,
    )
    return next(item for item in contributions if item.element.id == "service:frontend")


def _valid_metrics_payload() -> str:
    return """{
      "resourceMetrics": [{
        "scopeMetrics": [{
          "metrics": [{
            "name": "traces_service_graph_request_total",
            "sum": {
              "aggregationTemporality": 1,
              "dataPoints": [{
                "asInt": "1",
                "startTimeUnixNano": "1",
                "timeUnixNano": "1234567890",
                "attributes": [
                  {"key": "client", "value": {"stringValue": "frontend"}},
                  {"key": "server", "value": {"stringValue": "checkout-api"}}
                ]
              }]
            }
          }]
        }]
      }]
    }"""
