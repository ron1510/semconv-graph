package io.extendedotel.flink.model;

import java.math.BigInteger;
import java.util.Objects;

/** Complete replacement of one graph element. */
public record Upsert(
    String eventId,
    BigInteger observedAtUnixNano,
    long emittedAtUnixMs,
    String payloadHash,
    Element element)
    implements Event {
  public Upsert {
    eventId = ModelValues.required(eventId);
    ModelValues.positiveTimestamp(observedAtUnixNano);
    payloadHash = ModelValues.required(payloadHash);
    Objects.requireNonNull(element);
  }

  @Override
  public String operation() {
    return "upsert";
  }

  @Override
  public String elementId() {
    return element.id();
  }
}
