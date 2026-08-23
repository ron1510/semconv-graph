# JAAS escaping is an internal security boundary tested directly.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from otel_servicegraph_diff.config import (
    GraphEngineConfig,
    KafkaSaslMechanism,
    KafkaSecurityProtocol,
    _escape_jaas_value,
    graph_engine_config_from_env,
)


def test_environment_config_is_one_snapshot_per_process(monkeypatch: pytest.MonkeyPatch) -> None:
    graph_engine_config_from_env.cache_clear()
    try:
        monkeypatch.setenv("FLINK_PARALLELISM", "2")
        first = graph_engine_config_from_env()
        monkeypatch.setenv("FLINK_PARALLELISM", "4")

        assert graph_engine_config_from_env() is first
        assert first.parallelism == 2

        graph_engine_config_from_env.cache_clear()
        assert graph_engine_config_from_env().parallelism == 4
    finally:
        graph_engine_config_from_env.cache_clear()


def test_config_rejects_invalid_topic_name() -> None:
    with pytest.raises(ValidationError):
        GraphEngineConfig(output_topic="contains spaces")


def test_entity_event_source_is_opt_in_with_independent_group_and_grace() -> None:
    disabled = GraphEngineConfig()
    enabled = GraphEngineConfig(
        entity_input_topic="otel.entity.events",
        entity_group_id="entity-consumers",
        entity_report_interval_grace_seconds=15,
    )

    assert disabled.entity_input_topic is None
    assert enabled.entity_input_topic == "otel.entity.events"
    assert enabled.entity_group_id == "entity-consumers"
    assert enabled.entity_report_interval_grace_seconds == 15


@pytest.mark.parametrize("topic", ["otel.servicegraph.metrics", "graph.elements.events"])
def test_entity_event_topic_must_not_overlap_existing_contract(topic: str) -> None:
    with pytest.raises(ValidationError, match="must differ"):
        GraphEngineConfig(entity_input_topic=topic)


def test_config_rejects_non_positive_parallelism() -> None:
    with pytest.raises(ValidationError):
        GraphEngineConfig(parallelism=0)


def test_plaintext_config_has_only_explicit_security_protocol() -> None:
    config = GraphEngineConfig()

    assert config.kafka_client_properties == {"security.protocol": "PLAINTEXT"}


@pytest.mark.parametrize(
    "protocol",
    [KafkaSecurityProtocol.SASL_PLAINTEXT, KafkaSecurityProtocol.SASL_SSL],
)
def test_sasl_config_builds_complete_kafka_properties(protocol: KafkaSecurityProtocol) -> None:
    config = GraphEngineConfig(
        kafka_security_protocol=protocol,
        kafka_sasl_mechanism=KafkaSaslMechanism.SCRAM_SHA_256,
        kafka_sasl_username='service"graph',
        kafka_sasl_password=SecretStr('pass\\word'),
    )

    assert config.kafka_client_properties == {
        "security.protocol": protocol.value,
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.jaas.config": (
            "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
            'username="service\\"graph" password="pass\\\\word";'
        ),
    }


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"kafka_security_protocol": "SASL_SSL"}, "SASL mechanism"),
        (
            {
                "kafka_security_protocol": "SASL_PLAINTEXT",
                "kafka_sasl_mechanism": "SCRAM-SHA-256",
            },
            "username and password",
        ),
        ({"kafka_sasl_username": "invalid"}, "require SASL_PLAINTEXT or SASL_SSL"),
    ],
)
def test_config_rejects_incomplete_or_inconsistent_kafka_security(
    overrides: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        GraphEngineConfig.model_validate(overrides)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("plain", "plain"),
        ('service"graph', 'service\\"graph'),
        ("pass\\word", "pass\\\\word"),
        ('both\\"', 'both\\\\\\"'),
    ],
)
def test_jaas_values_escape_backslashes_before_quotes(value: str, expected: str) -> None:
    assert _escape_jaas_value(value) == expected
