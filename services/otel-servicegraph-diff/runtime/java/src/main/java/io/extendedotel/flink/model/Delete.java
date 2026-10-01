package io.extendedotel.flink.model;

import java.math.BigInteger;

/** Final removal of one graph element. */
public record Delete(
    String eventId, String elementId, BigInteger observedAtUnixNano, long emittedAtUnixMs)
    implements Event {
  public Delete {
    eventId = ModelValues.required(eventId);
    elementId = ModelValues.required(elementId);
    ModelValues.positiveTimestamp(observedAtUnixNano);
  }

  @Override
  public String operation() {
    return "delete";
  }
}
