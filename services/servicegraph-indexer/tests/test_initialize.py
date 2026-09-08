# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Mapping, Sequence
from unittest.mock import Mock

import pytest
from pydantic import SecretStr

import servicegraph_indexer.initialize as initialize_module
from servicegraph_indexer.initialize import (
    ArangoSettings,
    CollectionBoundary,
    GraphBoundary,
    TopologyInitializationError,
    arango_settings_from_env,
    ensure_topology,
    wait_for_database,
)
from servicegraph_indexer.schema import load_graph_schema


class FakeCollection:
    def __init__(self, collection_type: int) -> None:
        self.collection_type = collection_type
        self.index_values: list[dict[str, object]] = [
            {"name": "primary", "fields": ["_key"], "unique": True, "sparse": False}
        ]
        self.documents: dict[str, dict[str, object]] = {}

    def properties(self) -> Mapping[str, object]:
        return {"type": self.collection_type}

    def indexes(self) -> Sequence[Mapping[str, object]]:
        return self.index_values

    def add_persistent_index(
        self,
        *,
        fields: Sequence[str],
        unique: bool,
        sparse: bool,
        name: str,
    ) -> object:
        self.index_values.append({"name": name, "fields": list(fields), "unique": unique, "sparse": sparse})
        return self.index_values[-1]

    def get(self, document: str) -> Mapping[str, object] | None:
        return self.documents.get(document)

    def insert(self, document: Mapping[str, object]) -> object:
        stored = dict(document)
        self.documents[str(stored["_key"])] = stored
        return stored


class FakeGraph:
    def __init__(self, definitions: Sequence[Mapping[str, object]] = ()) -> None:
        self.definitions = [dict(item) for item in definitions]

    def edge_definitions(self) -> Sequence[Mapping[str, object]]:
        return self.definitions

    def create_edge_definition(
        self,
        *,
        edge_collection: str,
        from_vertex_collections: Sequence[str],
        to_vertex_collections: Sequence[str],
    ) -> object:
        definition: dict[str, object] = {
            "edge_collection": edge_collection,
            "from_vertex_collections": list(from_vertex_collections),
            "to_vertex_collections": list(to_vertex_collections),
        }
        self.definitions.append(definition)
        return definition


class FakeDatabase:
    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}
        self.graphs: dict[str, FakeGraph] = {}
        self.failures = 0

    def version(self) -> str:
        if self.failures:
            self.failures -= 1
            raise OSError("not ready")
        return "3.12.9-4"

    def has_collection(self, name: str) -> bool:
        return name in self.collections

    def create_collection(self, name: str, *, edge: bool = False) -> object:
        self.collections[name] = FakeCollection(3 if edge else 2)
        return self.collections[name]

    def collection(self, name: str) -> CollectionBoundary:
        return self.collections[name]

    def has_graph(self, name: str) -> bool:
        return name in self.graphs

    def create_graph(
        self,
        name: str,
        *,
        edge_definitions: Sequence[Mapping[str, object]],
        orphan_collections: Sequence[str],
    ) -> GraphBoundary:
        del orphan_collections
        self.graphs[name] = FakeGraph(edge_definitions)
        return self.graphs[name]

    def graph(self, name: str) -> GraphBoundary:
        return self.graphs[name]


def test_initializer_settings_are_cached_retry_failures_and_main_uses_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arango_settings_from_env.cache_clear()
    try:
        monkeypatch.setenv("SERVICEGRAPH_INDEXER_ARANGO_URLS", "invalid")
        monkeypatch.setenv("SERVICEGRAPH_INDEXER_ARANGO_PASSWORD", "secret")
        with pytest.raises(ValueError, match="http or https"):
            arango_settings_from_env()

        monkeypatch.setenv("SERVICEGRAPH_INDEXER_ARANGO_URLS", "http://one:8529")
        first = arango_settings_from_env()
        monkeypatch.setenv("SERVICEGRAPH_INDEXER_ARANGO_URLS", "http://two:8529")
        assert arango_settings_from_env() is first
        assert first.urls == ("http://one:8529",)

        initializer = Mock(return_value="unchanged")
        monkeypatch.setattr(initialize_module, "initialize", initializer)
        assert initialize_module.main() == 0
        initializer.assert_called_once_with(first)
    finally:
        arango_settings_from_env.cache_clear()


def test_initializer_creates_then_is_an_exact_noop() -> None:
    database = FakeDatabase()
    schema = load_graph_schema()

    assert ensure_topology(database, schema, "servicegraph") == "created"
    assert ensure_topology(database, schema, "servicegraph") == "unchanged"
    assert len(database.collections) == 42
    assert len(database.graphs["servicegraph"].definitions) == 9
    assert database.collections["TINKERPOP-GRAPH-VARIABLES"].documents == {
        "servicegraph": {"_key": "servicegraph", "_version": "4.0.0"}
    }
    assert any(index["name"] == "sg_element_id" for index in database.collections["service"].index_values)
    assert any(index["name"] == "sg_identity_service_name" for index in database.collections["service"].index_values)


def test_initializer_adds_missing_definition_and_rejects_mismatches() -> None:
    database = FakeDatabase()
    schema = load_graph_schema()
    ensure_topology(database, schema, "servicegraph")
    graph = database.graphs["servicegraph"]
    removed = graph.definitions.pop()

    assert ensure_topology(database, schema, "servicegraph") == "updated"
    assert removed in graph.definitions

    graph.definitions[0]["to_vertex_collections"] = ["service"]
    with pytest.raises(TopologyInitializationError, match="does not match"):
        ensure_topology(database, schema, "servicegraph")


def test_initializer_rejects_collection_and_index_mismatches() -> None:
    database = FakeDatabase()
    schema = load_graph_schema()
    database.collections["service"] = FakeCollection(3)
    with pytest.raises(TopologyInitializationError, match="not a document"):
        ensure_topology(database, schema, "servicegraph")

    database = FakeDatabase()
    ensure_topology(database, schema, "servicegraph")
    service = database.collections["service"]
    element_index = next(item for item in service.index_values if item["name"] == "sg_element_id")
    element_index["unique"] = False
    with pytest.raises(TopologyInitializationError, match="incompatible"):
        ensure_topology(database, schema, "servicegraph")


def test_wait_retries_and_settings_validate_urls() -> None:
    database = FakeDatabase()
    database.failures = 1
    moments = iter((0.0, 0.1, 0.2, 0.3))
    sleeps: list[float] = []

    assert wait_for_database(database, 1, monotonic=lambda: next(moments), sleep=sleeps.append) == "3.12.9-4"
    assert sleeps == [0.8]
    settings = ArangoSettings(arango_password=SecretStr("secret"), arango_urls="http://one:8529,http://two:8529")
    assert settings.urls == ("http://one:8529", "http://two:8529")
