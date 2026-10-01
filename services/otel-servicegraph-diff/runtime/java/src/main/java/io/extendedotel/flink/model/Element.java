package io.extendedotel.flink.model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** A semantic graph node or edge. */
public sealed interface Element extends Serializable permits Node, Edge {
  String id();

  String type();

  Map<String, Object> attributes();

  String kind();

  default String sourceId() {
    return null;
  }

  default String targetId() {
    return null;
  }

  static Node node(String id, String type, Map<String, Object> attributes) {
    return new Node(id, type, attributes);
  }

  static Edge edge(
      String id, String type, String sourceId, String targetId, Map<String, Object> attributes) {
    return new Edge(id, type, sourceId, targetId, attributes);
  }

  default Element withAttributes(Map<String, Object> attributes) {
    return this instanceof Node
        ? node(id(), type(), attributes)
        : edge(id(), type(), sourceId(), targetId(), attributes);
  }

  default Map<String, Object> toMap() {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("kind", kind());
    value.put("id", id());
    value.put("type", type());
    value.put("attributes", ModelValues.jsonValue(attributes()));
    if (this instanceof Edge edge) {
      value.put("source_id", edge.sourceId());
      value.put("target_id", edge.targetId());
    }
    return value;
  }

  static Element fromMap(Map<String, Object> value) {
    if (value.containsKey("metrics")) {
      throw new IllegalArgumentException("edge metrics are not supported");
    }
    String id = ModelValues.string(value, "id");
    String type = ModelValues.string(value, "type");
    Map<String, Object> attributes = ModelValues.object(value.getOrDefault("attributes", Map.of()));
    return switch (ModelValues.string(value, "kind")) {
      case "node" -> {
        if (value.get("source_id") != null || value.get("target_id") != null) {
          throw new IllegalArgumentException("invalid node shape");
        }
        yield node(id, type, attributes);
      }
      case "edge" ->
          edge(
              id,
              type,
              ModelValues.string(value, "source_id"),
              ModelValues.string(value, "target_id"),
              attributes);
      default -> throw new IllegalArgumentException("unknown graph element kind");
    };
  }
}
