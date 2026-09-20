"""Project Flink graph-element lifecycle events into ArangoDB."""

# External clients expose dynamic types at these adapter boundaries.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportMissingModuleSource=false

from __future__ import annotations

import hashlib
import json
import logging
import signal
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from functools import cache
from threading import Event
from types import FrameType
from typing import Any, Protocol, cast

from arango.database import StandardDatabase
from arango.exceptions import ArangoServerError
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition
from pydantic import Field, SecretStr, model_validator

from servicegraph_indexer.initialize import ArangoSettings, DatabaseBoundary, create_database, initialize
from servicegraph_indexer.schema import GraphSchema, load_graph_schema

LOGGER = logging.getLogger(__name__)
MAX_POLL_RECORDS = 500
POLL_TIMEOUT_MS = 1_000
DOCUMENT_NOT_FOUND = 1202


class IndexingError(RuntimeError):
    """Raised when a Kafka poll cannot be committed safely."""


class KafkaSecurityProtocol(StrEnum):
    PLAINTEXT = "PLAINTEXT"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SASL_SSL = "SASL_SSL"


class IndexerSettings(ArangoSettings):
    kafka_bootstrap_servers: str = Field(default="kafka:9092", min_length=1)
    kafka_security_protocol: KafkaSecurityProtocol = KafkaSecurityProtocol.PLAINTEXT
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = Field(default=None, min_length=1)
    kafka_sasl_password: SecretStr | None = None
    input_topic: str = Field(default="graph.elements.events", min_length=1, pattern=r"^[A-Za-z0-9._-]+$")
    consumer_group_id: str = Field(
        default="servicegraph-arangodb-indexer",
        min_length=1,
        pattern=r"^[A-Za-z0-9._-]+$",
    )

    @model_validator(mode="after")
    def validate_kafka_security(self) -> IndexerSettings:
        security_values = (self.kafka_sasl_mechanism, self.kafka_sasl_username, self.kafka_sasl_password)
        if self.kafka_security_protocol is not KafkaSecurityProtocol.PLAINTEXT:
            if self.kafka_sasl_mechanism != "SCRAM-SHA-256":
                raise ValueError("Kafka SASL authentication requires SCRAM-SHA-256")
            if self.kafka_sasl_username is None or self.kafka_sasl_password is None:
                raise ValueError("Kafka username and password are required for SASL authentication")
        elif any(value is not None for value in security_values):
            raise ValueError("Kafka authentication fields require SASL_PLAINTEXT or SASL_SSL")
        return self


@cache
def indexer_settings_from_env() -> IndexerSettings:
    return IndexerSettings()  # pyright: ignore[reportCallIssue]


class KafkaRecord(Protocol):
    offset: int
    value: bytes | str


class ConsumerBoundary(Protocol):
    def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata]) -> object: ...


class WriterBoundary(Protocol):
    def replace_many(self, collection: str, documents: Sequence[Mapping[str, object]]) -> None: ...

    def delete_many(self, collection: str, keys: Sequence[str]) -> None: ...


@dataclass(frozen=True)
class PendingEvent:
    element_id: str
    operation: str
    event: Mapping[str, object]


class ArangoWriter:
    def __init__(self, database: StandardDatabase) -> None:
        self._database = database

    def replace_many(self, collection: str, documents: Sequence[Mapping[str, object]]) -> None:
        self._database.collection(collection).insert_many(
            cast(Sequence[dict[str, Any]], documents),
            overwrite_mode="replace",
            silent=True,
            raise_on_document_error=True,
        )

    def delete_many(self, collection: str, keys: Sequence[str]) -> None:
        results = self._database.collection(collection).delete_many(
            [{"_key": key} for key in keys],
            check_rev=False,
            raise_on_document_error=False,
        )
        if results is True:
            return
        if not isinstance(results, list):
            raise IndexingError(f"ArangoDB returned an invalid delete result for {collection}: {results!r}")
        for result in results:
            if isinstance(result, ArangoServerError) and result.error_code != DOCUMENT_NOT_FOUND:
                raise IndexingError(f"ArangoDB delete failed in {collection}: {result}")


def element_key(element_id: str) -> str:
    return hashlib.sha256(element_id.encode("utf-8")).hexdigest()


def semantic_type_from_element_id(element_id: str) -> str:
    semantic_type, separator, _ = element_id.partition(":")
    if not separator or not semantic_type:
        raise IndexingError(f"node element ID has no semantic type prefix: {element_id!r}")
    return semantic_type


def event_to_document(event: Mapping[str, object], schema: GraphSchema) -> tuple[str, dict[str, object]]:
    _validate_event_contract(event)
    if event.get("operation") != "upsert":
        raise IndexingError("only upsert events can be converted to documents")
    element_value = event.get("element")
    if not isinstance(element_value, Mapping):
        raise IndexingError("upsert event has no element object")
    element = cast(Mapping[str, object], element_value)
    if "metrics" in element:
        raise IndexingError("graph schema 3 elements must not contain metrics")
    element_id = event.get("element_id")
    kind = element.get("kind")
    semantic_type = element.get("type")
    if not isinstance(element_id, str) or not isinstance(semantic_type, str):
        raise IndexingError("upsert event has invalid element identity")
    if kind == "node":
        definition = schema.vertices_by_type.get(semantic_type)
    elif kind == "edge":
        definition = schema.edges_by_type.get(semantic_type)
    else:
        raise IndexingError(f"unsupported graph element kind: {kind!r}")
    if definition is None:
        raise IndexingError(f"unknown {kind} semantic type: {semantic_type!r}")

    attributes = _object_map(element.get("attributes", {}), "attributes")
    document: dict[str, object] = {
        "_key": element_key(element_id),
        "element_id": element_id,
        "semantic_type": semantic_type,
        "attributes": attributes,
        "schema_version": event["schema_version"],
        "event_id": event["event_id"],
        "payload_hash": event["payload_hash"],
        "observed_at_unix_nano": event["observed_at_unix_nano"],
        "emitted_at_unix_ms": event["emitted_at_unix_ms"],
    }
    for canonical, alias in schema.property_aliases.attributes.items():
        if canonical in attributes:
            document[alias] = attributes[canonical]
    if kind == "edge":
        source_id = element.get("source_id")
        target_id = element.get("target_id")
        if not isinstance(source_id, str) or not isinstance(target_id, str):
            raise IndexingError("edge upsert has invalid endpoints")
        vertices = schema.vertices_by_type
        source_type = semantic_type_from_element_id(source_id)
        target_type = semantic_type_from_element_id(target_id)
        if source_type not in vertices or target_type not in vertices:
            raise IndexingError(f"edge references unknown endpoint types: {source_type!r}, {target_type!r}")
        document.update(
            {
                "_from": f"{vertices[source_type].collection}/{element_key(source_id)}",
                "_to": f"{vertices[target_type].collection}/{element_key(target_id)}",
                "source_id": source_id,
                "target_id": target_id,
            }
        )
    return definition.collection, document


def create_consumer(settings: IndexerSettings) -> KafkaConsumer:
    if settings.kafka_security_protocol is KafkaSecurityProtocol.PLAINTEXT:
        return KafkaConsumer(
            settings.input_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.consumer_group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=None,
            security_protocol="PLAINTEXT",
        )
    password = settings.kafka_sasl_password
    if password is None:
        raise RuntimeError("validated Kafka SASL settings are incomplete")
    return KafkaConsumer(
        settings.input_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.consumer_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=None,
        security_protocol=settings.kafka_security_protocol.value,
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=settings.kafka_sasl_username,
        sasl_plain_password=password.get_secret_value(),
    )


def project_poll(
    records: Mapping[TopicPartition, Sequence[KafkaRecord]],
    consumer: ConsumerBoundary,
    writer: WriterBoundary,
    schema: GraphSchema,
) -> int:
    pending: dict[str, PendingEvent] = {}
    offsets: dict[TopicPartition, OffsetAndMetadata] = {}
    count = 0
    for topic_partition, partition_records in records.items():
        for record in partition_records:
            event = _decode_event(record.value)
            element_id = event.get("element_id")
            operation = event.get("operation")
            if not isinstance(element_id, str) or operation not in {"upsert", "delete"}:
                raise IndexingError("graph-element event has invalid operation or element_id")
            pending[element_id] = PendingEvent(element_id, cast(str, operation), event)
            count += 1
        if partition_records:
            offsets[topic_partition] = OffsetAndMetadata(
                partition_records[-1].offset + 1,
                "",
            )  # pyright: ignore[reportCallIssue] - kafka-python-ng 2.2 runtime has two fields.

    upserts: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    node_deletes: dict[str, list[str]] = defaultdict(list)
    edge_delete_keys: list[str] = []
    for item in pending.values():
        key = element_key(item.element_id)
        if item.operation == "upsert":
            collection, document = event_to_document(item.event, schema)
            upserts[collection].append(document)
        elif item.element_id.startswith("edge:"):
            edge_delete_keys.append(key)
        else:
            semantic_type = semantic_type_from_element_id(item.element_id)
            vertex = schema.vertices_by_type.get(semantic_type)
            if vertex is None:
                raise IndexingError(f"node delete has unknown semantic type: {semantic_type!r}")
            node_deletes[vertex.collection].append(key)

    for collection, documents in upserts.items():
        writer.replace_many(collection, documents)
    for collection, keys in node_deletes.items():
        writer.delete_many(collection, keys)
    if edge_delete_keys:
        for edge in schema.edge_collections:
            writer.delete_many(edge.collection, edge_delete_keys)
    if offsets:
        consumer.commit(offsets=offsets)
    return count


def run_indexer(
    settings: IndexerSettings,
    *,
    stop: Event | None = None,
    consumer: KafkaConsumer | None = None,
    database: DatabaseBoundary | None = None,
) -> None:
    stop_event = stop or Event()
    active_database = database or create_database(settings)
    initialize(settings, active_database)
    schema = load_graph_schema()
    writer = ArangoWriter(cast(StandardDatabase, active_database))
    owned_consumer = consumer is None
    active_consumer = consumer or create_consumer(settings)
    try:
        LOGGER.info("Indexing %s with consumer group %s", settings.input_topic, settings.consumer_group_id)
        while not stop_event.is_set():
            records = active_consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=MAX_POLL_RECORDS)
            indexed = project_poll(
                cast(Mapping[TopicPartition, Sequence[KafkaRecord]], records),
                active_consumer,
                writer,
                schema,
            )
            if indexed:
                LOGGER.info("Indexed and committed %d graph-element events", indexed)
    finally:
        if owned_consumer:
            active_consumer.close(autocommit=False)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop = Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        del signum, frame
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        run_indexer(indexer_settings_from_env(), stop=stop)
    except Exception:
        LOGGER.exception("Service graph indexing failed")
        return 1
    return 0


def _decode_event(payload: bytes | str) -> Mapping[str, object]:
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise IndexingError("graph-element event must be a JSON object")
    event = cast(Mapping[str, object], decoded)
    _validate_event_contract(event)
    return event


def _validate_event_contract(event: Mapping[str, object]) -> None:
    if event.get("schema_version") != "3.0":
        raise IndexingError("indexer accepts only graph-event schema 3.0")
    if event.get("event_type") != "graph_element_state_changed":
        raise IndexingError("unsupported graph-element event type")


def _object_map(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise IndexingError(f"graph element {field} must be an object")
    typed_value = cast(Mapping[object, object], value)
    return {str(name): item for name, item in typed_value.items()}


if __name__ == "__main__":
    raise SystemExit(main())
