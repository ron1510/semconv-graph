package io.extendedotel.flink.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Pure lifecycle state for one graph element. */
public record State(String elementId, Map<String, Snapshot> contributors, String lastPayloadHash) {
  public State {
    contributors = Collections.unmodifiableMap(new LinkedHashMap<>(contributors));
  }

  public Aggregate aggregate() {
    return new Aggregate(elementId, lastPayloadHash);
  }

  public Map<String, Object> toMap() {
    Map<String, Object> snapshots = new LinkedHashMap<>();
    contributors.forEach((id, snapshot) -> snapshots.put(id, snapshot.toMap()));
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("element_id", elementId);
    value.put("contributors", snapshots);
    value.put("last_payload_hash", lastPayloadHash);
    return value;
  }
}
