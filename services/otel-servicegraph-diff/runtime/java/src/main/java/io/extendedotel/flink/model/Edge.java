package io.extendedotel.flink.model;

import java.util.Map;

/** A directed semantic relationship. */
public record Edge(
    String id, String type, String sourceId, String targetId, Map<String, Object> attributes)
    implements Element {
  public Edge {
    id = ModelValues.required(id);
    type = ModelValues.required(type);
    sourceId = ModelValues.required(sourceId);
    targetId = ModelValues.required(targetId);
    attributes = ModelValues.immutableObject(attributes);
  }

  @Override
  public String kind() {
    return "edge";
  }
}
