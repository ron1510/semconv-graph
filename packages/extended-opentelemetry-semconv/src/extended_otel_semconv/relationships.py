"""Runtime semantic relationship definitions."""

from __future__ import annotations

import json
from functools import cache
from importlib.resources import files
from typing import Literal

from pydantic import BaseModel, ConfigDict, TypeAdapter


class RelationshipDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    type: str
    name: str
    source_entity: str
    target_entity: str
    source_signals: tuple[Literal["trace", "service_graph", "span_metrics"], ...]
    evidence_scope: Literal["observation", "interaction"]
    connection_type: str | None = None
    stability: str | None = None
    brief: str | None = None


@cache
def graph_relationships() -> tuple[RelationshipDefinition, ...]:
    metadata = files("extended_otel_semconv").joinpath("metadata", "graph-relationships.json")
    document = json.loads(metadata.read_text(encoding="utf-8"))
    return TypeAdapter(tuple[RelationshipDefinition, ...]).validate_python(document)
