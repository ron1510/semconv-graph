package io.extendedotel.flink.model;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;

/** A contribution with its two expiry deadlines. */
public record Snapshot(
    BigInteger observedAtUnixNano,
    BigInteger eventExpiresAtUnixNano,
    Long processingExpiresAtUnixMs,
    Element element)
    implements Serializable {
  public Map<String, Object> toMap() {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("observed_at_unix_nano", observedAtUnixNano);
    value.put("event_expires_at_unix_nano", eventExpiresAtUnixNano);
    value.put("processing_expires_at_unix_ms", processingExpiresAtUnixMs);
    value.put("element", element.toMap());
    return value;
  }

  public static Snapshot fromMap(Map<String, Object> value) {
    return new Snapshot(
        ModelValues.exactInteger(value.get("observed_at_unix_nano")),
        ModelValues.optionalInteger(value.get("event_expires_at_unix_nano")),
        ModelValues.optionalLong(value.get("processing_expires_at_unix_ms")),
        Element.fromMap(ModelValues.object(value.get("element"))));
  }
}
