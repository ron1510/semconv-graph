package io.extendedotel.flink.model;

import java.util.Map;

/** A semantic graph entity. */
public record Node(String id, String type, Map<String, Object> attributes) implements Element {
  public Node {
    id = ModelValues.required(id);
    type = ModelValues.required(type);
    attributes = ModelValues.immutableObject(attributes);
  }

  @Override
  public String kind() {
    return "node";
  }
}
