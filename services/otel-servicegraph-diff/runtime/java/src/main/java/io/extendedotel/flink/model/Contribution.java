package io.extendedotel.flink.model;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** One contributor's observation of a graph element. */
public record Contribution(String contributorId, BigInteger observedAtUnixNano, Element element)
    implements Serializable {
  public Contribution(String contributorId, long observedAtUnixNano, Element element) {
    this(contributorId, BigInteger.valueOf(observedAtUnixNano), element);
  }

  public Contribution {
    contributorId = ModelValues.required(contributorId);
    ModelValues.positiveTimestamp(observedAtUnixNano);
    Objects.requireNonNull(element);
  }

  public String elementKey() {
    return element.id();
  }

  public Map<String, Object> toMap() {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("contributor_id", contributorId);
    value.put("observed_at_unix_nano", observedAtUnixNano);
    value.put("element", element.toMap());
    return value;
  }

  public static Contribution fromMap(Map<String, Object> value) {
    return new Contribution(
        ModelValues.string(value, "contributor_id"),
        ModelValues.exactInteger(value.get("observed_at_unix_nano")),
        Element.fromMap(ModelValues.object(value.get("element"))));
  }
}
