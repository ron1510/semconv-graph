package io.extendedotel.flink;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Typed graph elements, evidence contributions, lifecycle state, and schema-3 events. */
public final class GraphModel {
  private GraphModel() {}

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
      value.put("attributes", jsonValue(attributes()));
      if (this instanceof Edge edge) {
        value.put("source_id", edge.sourceId());
        value.put("target_id", edge.targetId());
      }
      return value;
    }

    static Element fromMap(Map<String, Object> value) {
      if (value.containsKey("metrics"))
        throw new IllegalArgumentException("edge metrics are not supported");
      String id = string(value, "id"), type = string(value, "type");
      Map<String, Object> attributes = object(value.getOrDefault("attributes", Map.of()));
      return switch (string(value, "kind")) {
        case "node" -> {
          if (value.get("source_id") != null || value.get("target_id") != null)
            throw new IllegalArgumentException("invalid node shape");
          yield node(id, type, attributes);
        }
        case "edge" ->
            edge(id, type, string(value, "source_id"), string(value, "target_id"), attributes);
        default -> throw new IllegalArgumentException("unknown graph element kind");
      };
    }
  }

  public record Node(String id, String type, Map<String, Object> attributes) implements Element {
    public Node {
      id = required(id);
      type = required(type);
      attributes = immutableObject(attributes);
    }

    @Override
    public String kind() {
      return "node";
    }
  }

  public record Edge(
      String id, String type, String sourceId, String targetId, Map<String, Object> attributes)
      implements Element {
    public Edge {
      id = required(id);
      type = required(type);
      sourceId = required(sourceId);
      targetId = required(targetId);
      attributes = immutableObject(attributes);
    }

    @Override
    public String kind() {
      return "edge";
    }
  }

  public record Contribution(String contributorId, BigInteger observedAtUnixNano, Element element)
      implements Serializable {
    public Contribution(String contributorId, long observedAtUnixNano, Element element) {
      this(contributorId, BigInteger.valueOf(observedAtUnixNano), element);
    }

    public Contribution {
      contributorId = required(contributorId);
      positiveTimestamp(observedAtUnixNano);
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
          string(value, "contributor_id"),
          exactInteger(value.get("observed_at_unix_nano")),
          Element.fromMap(object(value.get("element"))));
    }
  }

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
          exactInteger(value.get("observed_at_unix_nano")),
          optionalInteger(value.get("event_expires_at_unix_nano")),
          optionalLong(value.get("processing_expires_at_unix_ms")),
          Element.fromMap(object(value.get("element"))));
    }
  }

  public record State(
      String elementId, Map<String, Snapshot> contributors, String lastPayloadHash) {
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

  public record Aggregate(String elementId, String lastPayloadHash) {
    public Map<String, Object> toMap() {
      return Map.of("element_id", elementId, "last_payload_hash", lastPayloadHash);
    }

    public static Aggregate fromMap(Map<String, Object> value) {
      return new Aggregate(string(value, "element_id"), string(value, "last_payload_hash"));
    }
  }

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

    default String toJson() {
      return CanonicalJson.stringify(toMap());
    }

    static Event fromMap(Map<String, Object> value) {
      if (!"3.0".equals(value.get("schema_version"))
          || !"graph_element_state_changed".equals(value.get("event_type")))
        throw new IllegalArgumentException("unsupported graph event contract");
      String id = string(value, "event_id"), elementId = string(value, "element_id");
      BigInteger timestamp = exactInteger(value.get("observed_at_unix_nano"));
      long emitted = exactLong(value.get("emitted_at_unix_ms"));
      return switch (string(value, "operation")) {
        case "upsert" -> {
          Element element = Element.fromMap(object(value.get("element")));
          if (!element.id().equals(elementId))
            throw new IllegalArgumentException("upsert payload has another element identity");
          yield new Upsert(id, timestamp, emitted, string(value, "payload_hash"), element);
        }
        case "delete" -> {
          if (value.get("element") != null || value.get("payload_hash") != null)
            throw new IllegalArgumentException("delete must have no payload");
          yield new Delete(id, elementId, timestamp, emitted);
        }
        default -> throw new IllegalArgumentException("unknown lifecycle operation");
      };
    }
  }

  public record Upsert(
      String eventId,
      BigInteger observedAtUnixNano,
      long emittedAtUnixMs,
      String payloadHash,
      Element element)
      implements Event {
    public Upsert {
      eventId = required(eventId);
      positiveTimestamp(observedAtUnixNano);
      payloadHash = required(payloadHash);
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

  public record Delete(
      String eventId, String elementId, BigInteger observedAtUnixNano, long emittedAtUnixMs)
      implements Event {
    public Delete {
      eventId = required(eventId);
      elementId = required(elementId);
      positiveTimestamp(observedAtUnixNano);
    }

    @Override
    public String operation() {
      return "delete";
    }
  }

  public record Result(State state, Event event) {}

  public static Map<String, Object> object(Object value) {
    if (!(value instanceof Map<?, ?> map))
      throw new IllegalArgumentException("expected JSON object");
    Map<String, Object> result = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (!(entry.getKey() instanceof String key))
        throw new IllegalArgumentException("expected string object key");
      result.put(key, entry.getValue());
    }
    return result;
  }

  public static long exactLong(Object value) {
    return exactInteger(value).longValueExact();
  }

  public static BigInteger exactInteger(Object value) {
    if (value instanceof BigInteger integer) return integer;
    if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long) return BigInteger.valueOf(((Number) value).longValue());
    throw new IllegalArgumentException("expected exact integer");
  }

  private static BigInteger optionalInteger(Object value) {
    return value == null ? null : exactInteger(value);
  }

  private static Long optionalLong(Object value) {
    return value == null ? null : exactLong(value);
  }

  private static String string(Map<String, Object> value, String name) {
    if (!(value.get(name) instanceof String string))
      throw new IllegalArgumentException("expected string field " + name);
    return string;
  }

  private static void positiveTimestamp(BigInteger value) {
    if (Objects.requireNonNull(value).signum() <= 0)
      throw new IllegalArgumentException("observation nanoseconds must be positive");
  }

  private static String required(String value) {
    if (value == null || value.strip().isEmpty())
      throw new IllegalArgumentException("expected nonempty string");
    return value.strip();
  }

  private static Object jsonValue(Object value) {
    if (value instanceof Double number && !Double.isFinite(number)) return null;
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> result = new LinkedHashMap<>();
      map.forEach((key, item) -> result.put((String) key, jsonValue(item)));
      return result;
    }
    if (value instanceof List<?> list) {
      List<Object> result = new ArrayList<>();
      list.forEach(item -> result.add(jsonValue(item)));
      return result;
    }
    return value;
  }

  private static Map<String, Object> immutableObject(Map<String, Object> value) {
    Map<String, Object> result = new LinkedHashMap<>();
    value.forEach((name, item) -> result.put(name, immutable(item)));
    return Collections.unmodifiableMap(result);
  }

  private static Object immutable(Object value) {
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> result = new LinkedHashMap<>();
      map.forEach((key, item) -> result.put((String) key, immutable(item)));
      return Collections.unmodifiableMap(result);
    }
    if (value instanceof List<?> list) return list.stream().map(GraphModel::immutable).toList();
    return value;
  }
}
