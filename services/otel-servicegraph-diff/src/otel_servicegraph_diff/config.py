"""Validated settings for the PyFlink graph-element service."""

from __future__ import annotations

from enum import StrEnum
from functools import cache
from typing import Annotated

from pydantic import Field, SecretStr, StringConstraints, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

TopicName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, pattern=r"^[A-Za-z0-9._-]+$")]


class KafkaSecurityProtocol(StrEnum):
    PLAINTEXT = "PLAINTEXT"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SASL_SSL = "SASL_SSL"


class KafkaSaslMechanism(StrEnum):
    SCRAM_SHA_256 = "SCRAM-SHA-256"


class GraphEngineConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    kafka_bootstrap_servers: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)] = Field(
        default="kafka:9092",
        validation_alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    kafka_security_protocol: KafkaSecurityProtocol = Field(
        default=KafkaSecurityProtocol.PLAINTEXT,
        validation_alias="KAFKA_SECURITY_PROTOCOL",
    )
    kafka_sasl_mechanism: KafkaSaslMechanism | None = Field(
        default=None,
        validation_alias="KAFKA_SASL_MECHANISM",
    )
    kafka_sasl_username: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)] | None = Field(
        default=None,
        validation_alias="KAFKA_SASL_USERNAME",
    )
    kafka_sasl_password: SecretStr | None = Field(
        default=None,
        validation_alias="KAFKA_SASL_PASSWORD",
    )
    input_topic: TopicName = Field(
        default="otel.servicegraph.metrics",
        validation_alias="INTERACTION_DIFF_INPUT_TOPIC",
    )
    entity_input_topic: TopicName | None = Field(
        default=None,
        validation_alias="ENTITY_EVENTS_INPUT_TOPIC",
    )
    output_topic: TopicName = Field(
        default="graph.elements.events",
        validation_alias="INTERACTION_DIFF_OUTPUT_TOPIC",
    )
    group_id: TopicName = Field(
        default="graph-element-engine",
        validation_alias="INTERACTION_DIFF_GROUP_ID",
    )
    entity_group_id: TopicName = Field(
        default="graph-element-engine-entities",
        validation_alias="ENTITY_EVENTS_GROUP_ID",
    )
    entity_report_interval_grace_seconds: int = Field(
        default=30,
        ge=0,
        validation_alias="ENTITY_EVENTS_REPORT_INTERVAL_GRACE_SECONDS",
    )
    contributor_ttl_seconds: int = Field(default=300, gt=0, validation_alias="INTERACTION_DIFF_TTL_SECONDS")
    allowed_lateness_seconds: int = Field(
        default=60,
        ge=0,
        validation_alias="INTERACTION_DIFF_ALLOWED_LATENESS_SECONDS",
    )
    state_ttl_seconds: int = Field(default=86_400, gt=0, validation_alias="INTERACTION_DIFF_STATE_TTL_SECONDS")
    checkpoint_interval_ms: int = Field(default=30_000, ge=1_000, validation_alias="FLINK_CHECKPOINT_INTERVAL_MS")
    parallelism: int = Field(default=3, gt=0, validation_alias="FLINK_PARALLELISM")
    restart_attempts: int = Field(default=3, ge=0, validation_alias="FLINK_RESTART_ATTEMPTS")
    restart_delay_seconds: int = Field(default=10, ge=0, validation_alias="FLINK_RESTART_DELAY_SECONDS")

    @model_validator(mode="after")
    def validate_ttl_relationship(self) -> GraphEngineConfig:
        minimum_state_ttl = self.contributor_ttl_seconds + self.allowed_lateness_seconds
        if self.state_ttl_seconds <= minimum_state_ttl:
            raise ValueError("state TTL must exceed contributor TTL plus allowed lateness")
        if self.entity_input_topic is not None and self.entity_input_topic in {
            self.input_topic,
            self.output_topic,
        }:
            raise ValueError("entity-event input topic must differ from metrics input and graph output topics")
        if self.kafka_security_protocol is not KafkaSecurityProtocol.PLAINTEXT:
            if self.kafka_sasl_mechanism is None:
                raise ValueError("SASL mechanism is required when Kafka uses authentication")
            if self.kafka_sasl_username is None or self.kafka_sasl_password is None:
                raise ValueError("SASL username and password are required when Kafka uses authentication")
        elif any(
            value is not None
            for value in (
                self.kafka_sasl_mechanism,
                self.kafka_sasl_username,
                self.kafka_sasl_password,
            )
        ):
            raise ValueError("Kafka authentication fields require SASL_PLAINTEXT or SASL_SSL")
        return self

    @property
    def bootstrap_servers(self) -> str:
        return self.kafka_bootstrap_servers

    @property
    def kafka_client_properties(self) -> dict[str, str]:
        properties = {"security.protocol": self.kafka_security_protocol.value}
        if self.kafka_security_protocol is KafkaSecurityProtocol.PLAINTEXT:
            return properties

        mechanism = self.kafka_sasl_mechanism
        username = self.kafka_sasl_username
        password = self.kafka_sasl_password
        if mechanism is None or username is None or password is None:
            raise RuntimeError("validated Kafka SASL settings are incomplete")

        escaped_username = _escape_jaas_value(username)
        escaped_password = _escape_jaas_value(password.get_secret_value())
        return {
            **properties,
            "sasl.mechanism": mechanism.value,
            "sasl.jaas.config": (
                "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
                f'username="{escaped_username}" password="{escaped_password}";'
            ),
        }


def _escape_jaas_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


@cache
def graph_engine_config_from_env() -> GraphEngineConfig:
    return GraphEngineConfig()
