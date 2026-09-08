from __future__ import annotations

import pytest

import servicegraph_indexer.schema as schema_module
from servicegraph_indexer.schema import SchemaError, load_graph_schema


def test_generated_schema_is_valid_and_complete() -> None:
    schema = load_graph_schema()

    assert schema.graph_name == "servicegraph"
    assert len(schema.vertex_collections) == 32
    assert len(schema.edge_collections) == 9
    assert schema.vertices_by_type["etl.pipeline"].collection == "etl_pipeline"
    assert schema.vertices_by_type["etl.run"].collection == "etl_run"
    assert schema.vertices_by_type["etl.part.run"].collection == "etl_part_run"
    contains = schema.edges_by_type["contains"]
    assert "etl_pipeline" in contains.from_collections
    assert "etl_run" in contains.from_collections
    assert "etl_run" in contains.to_collections
    assert "etl_part_run" in contains.to_collections
    assert schema.vertices_by_type["service.instance"].collection == "service_instance"
    assert schema.vertices_by_type["k8s.pod"].collection == "k8s_pod"
    assert schema.edges_by_type["calls"].collection == "calls"
    assert schema.property_aliases.attributes["service.name"] == "service_name"
    assert schema.property_aliases.metrics["service_graph.request.total"] == "service_graph_request_total"


def test_generated_schema_and_lookup_maps_are_cached_and_read_only() -> None:
    schema = load_graph_schema()

    assert load_graph_schema() is schema
    assert schema.vertices_by_type is schema.vertices_by_type
    assert schema.edges_by_type is schema.edges_by_type
    with pytest.raises(TypeError):
        schema.property_aliases.attributes["invalid"] = "invalid"  # type: ignore[index]
    with pytest.raises(TypeError):
        schema.vertices_by_type["invalid"] = schema.vertex_collections[0]  # type: ignore[index]


def test_invalid_schema_is_not_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    class InvalidResource:
        def __init__(self) -> None:
            self.read_count = 0

        def joinpath(self, *parts: str) -> InvalidResource:
            del parts
            return self

        def read_text(self, *, encoding: str) -> str:
            del encoding
            self.read_count += 1
            return "{}"

    resource = InvalidResource()

    def invalid_files(package: object) -> InvalidResource:
        del package
        return resource

    load_graph_schema.cache_clear()
    monkeypatch.setattr(schema_module, "files", invalid_files)
    try:
        with pytest.raises(SchemaError, match="no _meta"):
            load_graph_schema()
        with pytest.raises(SchemaError, match="no _meta"):
            load_graph_schema()
        assert resource.read_count == 2
    finally:
        load_graph_schema.cache_clear()
