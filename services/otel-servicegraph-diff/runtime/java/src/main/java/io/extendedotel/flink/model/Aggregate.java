package io.extendedotel.flink.model;

import java.util.LinkedHashMap;
import java.util.Map;

/** Compact persisted identity and last emitted payload hash. */
public record Aggregate(String elementId, String lastPayloadHash) {
  public Map<String, Object> toMap() {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("element_id", elementId);
    value.put("last_payload_hash", lastPayloadHash);
    return value;
  }

  public static Aggregate fromMap(Map<String, Object> value) {
    return new Aggregate(
        ModelValues.string(value, "element_id"), ModelValues.string(value, "last_payload_hash"));
  }
}
