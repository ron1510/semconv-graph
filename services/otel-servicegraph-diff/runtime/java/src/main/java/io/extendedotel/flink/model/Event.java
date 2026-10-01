package io.extendedotel.flink.model;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;

/** A schema-3 public graph lifecycle event. */
public sealed interface Event extends Serializable permits Upsert, Delete {
  String eventId();

  String elementId();

  BigInteger observedAtUnixNano();

  long emittedAtUnixMs();

  String operation();

  default String payloadHash() {
    return null;
  }

  default Element element() {
    return null;
  }

  default Map<String, Object> toMap() {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("schema_version", "3.0");
    value.put("event_id", eventId());
    value.put("event_type", "graph_element_state_changed");
    value.put("element_id", elementId());
    value.put("observed_at_unix_nano", observedAtUnixNano());
    value.put("emitted_at_unix_ms", emittedAtUnixMs());
    value.put("operation", operation());
    value.put("payload_hash", payloadHash());
    value.put("element", element() == null ? null : element().toMap());
    return value;
  }

  static Event fromMap(Map<String, Object> value) {
    if (!"3.0".equals(value.get("schema_version"))
        || !"graph_element_state_changed".equals(value.get("event_type"))) {
      throw new IllegalArgumentException("unsupported graph event contract");
    }
    String id = ModelValues.string(value, "event_id");
    String elementId = ModelValues.string(value, "element_id");
    BigInteger timestamp = ModelValues.exactInteger(value.get("observed_at_unix_nano"));
    long emitted = ModelValues.exactLong(value.get("emitted_at_unix_ms"));
    return switch (ModelValues.string(value, "operation")) {
      case "upsert" -> {
        Element element = Element.fromMap(ModelValues.object(value.get("element")));
        if (!element.id().equals(elementId)) {
          throw new IllegalArgumentException("upsert payload has another element identity");
        }
        yield new Upsert(
            id, timestamp, emitted, ModelValues.string(value, "payload_hash"), element);
      }
      case "delete" -> {
        if (value.get("element") != null || value.get("payload_hash") != null) {
          throw new IllegalArgumentException("delete must have no payload");
        }
        yield new Delete(id, elementId, timestamp, emitted);
      }
      default -> throw new IllegalArgumentException("unknown lifecycle operation");
    };
  }
}
