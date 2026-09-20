package io.extendedotel.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Immutable generated registry metadata and the SDK's custom extraction rules. */
public final class SemanticRegistry {
  private record Field(String name, String kind, boolean required, List<Object> enumValues) {}

  private record Model(String type, List<String> identity, List<Field> fields) {}

  public record Relationship(String type, String source, String target) {}

  private final Map<String, Model> models;
  private final List<Relationship> relationships;
  private final Set<String> graphTypes;
  public static final SemanticRegistry INSTANCE = new SemanticRegistry();

  private SemanticRegistry() {
    var mapper = new ObjectMapper();
    var loaded = new LinkedHashMap<String, Model>();
    var relations = new ArrayList<Relationship>();
    try (var input = SemanticRegistry.class.getResourceAsStream("/semantic-registry.json")) {
      if (input == null) throw new IllegalStateException("generated semantic registry missing");
      JsonNode document = mapper.readTree(input);
      for (JsonNode entity : document.get("entities")) {
        var identity = new ArrayList<String>();
        entity.get("identity_fields").forEach(item -> identity.add(item.asText()));
        var fields = new ArrayList<Field>();
        for (JsonNode field : entity.get("fields")) {
          var values = new ArrayList<Object>();
          field
              .get("enum_values")
              .forEach(item -> values.add(mapper.convertValue(item, Object.class)));
          fields.add(
              new Field(
                  field.get("name").asText(),
                  field.get("kind").asText(),
                  field.get("required").asBoolean(),
                  List.copyOf(values)));
        }
        String type = entity.get("type").asText();
        loaded.put(type, new Model(type, List.copyOf(identity), List.copyOf(fields)));
      }
      for (JsonNode relation : document.get("relationships"))
        relations.add(
            new Relationship(
                relation.get("type").asText(),
                relation.get("source").asText(),
                relation.get("target").asText()));
    } catch (IOException error) {
      throw new IllegalStateException("cannot load semantic registry", error);
    }
    models = Map.copyOf(loaded);
    relationships = List.copyOf(relations);
    graphTypes =
        relations.stream()
            .flatMap(item -> java.util.stream.Stream.of(item.source(), item.target()))
            .collect(Collectors.toUnmodifiableSet());
  }

  public List<Relationship> relationships() {
    return relationships;
  }

  public Set<String> graphTypes() {
    return graphTypes;
  }

  public Set<String> knownElementTypes() {
    return java.util.stream.Stream.concat(
            models.keySet().stream(), relationships.stream().map(Relationship::type))
        .collect(Collectors.toUnmodifiableSet());
  }

  public boolean allows(String source, String target, String type) {
    return relationships.stream()
        .anyMatch(
            item ->
                item.source().equals(source)
                    && item.target().equals(target)
                    && item.type().equals(type));
  }

  public List<GraphModel.Element> extract(Map<String, Object> attributes) {
    var result = new ArrayList<GraphModel.Element>();
    for (String type : models.keySet().stream().sorted().toList()) {
      var entity = extract(type, attributes);
      if (entity != null) result.add(entity);
    }
    return result;
  }

  public GraphModel.Element require(String type, Map<String, Object> attributes) {
    var entity = extract(type, attributes);
    if (entity == null) throw new IllegalArgumentException("missing semantic identity fields");
    return entity;
  }

  private GraphModel.Element extract(String type, Map<String, Object> attributes) {
    Model model = models.get(type);
    if (model == null) throw new IllegalArgumentException("unknown semantic entity type");
    if (!attributes.keySet().containsAll(model.identity())) return null;
    var selected = new LinkedHashMap<String, Object>();
    for (Field field : model.fields()) {
      if (field.kind().endsWith("_map")) {
        String prefix = field.name() + ".";
        for (var entry : attributes.entrySet()) {
          if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
            selected.put(
                entry.getKey(),
                validate(
                    field.kind().substring(0, field.kind().length() - 4),
                    entry.getValue(),
                    false,
                    field.enumValues()));
          }
        }
      } else if (attributes.containsKey(field.name())) {
        Object value = attributes.get(field.name());
        if (value == null && !field.required()) continue;
        selected.put(
            field.name(), validate(field.kind(), value, field.required(), field.enumValues()));
      }
    }
    Object[] identity = model.identity().stream().map(selected::get).toArray();
    return GraphModel.Element.node(quotedId(type, identity), type, selected);
  }

  private static Object validate(
      String kind, Object value, boolean required, List<Object> enumValues) {
    if (kind.endsWith("_array")) {
      if (!(value instanceof List<?> items))
        throw new IllegalArgumentException("invalid semantic array");
      return items.stream()
          .map(item -> validate(kind.substring(0, kind.length() - 6), item, false, enumValues))
          .toList();
    }
    if (kind.equals("enum")) {
      for (Object registered : enumValues) {
        if (registered.equals(value) || numericEquivalent(registered, value)) return registered;
      }
      throw new IllegalArgumentException("invalid modeled semantic enum attribute");
    }
    boolean valid =
        switch (kind) {
          case "string" -> value instanceof String text && (!required || !text.isEmpty());
          case "integer" ->
              value instanceof Long
                  || value instanceof Integer
                  || value instanceof java.math.BigInteger;
          case "number" -> value instanceof Number number && Double.isFinite(number.doubleValue());
          case "boolean" -> value instanceof Boolean;
          default -> false;
        };
    if (!valid) throw new IllegalArgumentException("invalid modeled semantic attribute");
    return kind.equals("number") ? ((Number) value).doubleValue() : value;
  }

  private static boolean numericEquivalent(Object left, Object right) {
    if (!(left instanceof Number || left instanceof Boolean)
        || !(right instanceof Number || right instanceof Boolean)) return false;
    if (left instanceof Number leftNumber && !Double.isFinite(leftNumber.doubleValue())
        || right instanceof Number rightNumber && !Double.isFinite(rightNumber.doubleValue()))
      return false;
    String a = left instanceof Boolean flag ? (flag ? "1" : "0") : left.toString();
    String b = right instanceof Boolean flag ? (flag ? "1" : "0") : right.toString();
    return new java.math.BigDecimal(a).compareTo(new java.math.BigDecimal(b)) == 0;
  }

  public Map<String, Object> normalizeIdentity(String type, Map<String, String> identity) {
    Model model = models.get(type);
    if (model == null || !Set.copyOf(model.identity()).equals(identity.keySet()))
      throw new IllegalArgumentException("entity.id must exactly match registered identity fields");
    var result = new LinkedHashMap<String, Object>();
    for (Field field : model.fields()) {
      if (!field.required()) continue;
      String value = identity.get(field.name());
      Object normalized = value;
      if (field.kind().equals("integer")) {
        var parsed = new java.math.BigInteger(value);
        if (!parsed.toString().equals(value))
          throw new IllegalArgumentException("noncanonical integer identity");
        normalized = parsed;
      } else if (field.kind().equals("number")) {
        double parsed = Double.parseDouble(value);
        if (!Double.isFinite(parsed) || !CanonicalJson.stringify(parsed).equals(value))
          throw new IllegalArgumentException("noncanonical number identity");
        normalized = parsed;
      } else if (field.kind().equals("boolean")) {
        if (!value.equals("true") && !value.equals("false"))
          throw new IllegalArgumentException("invalid boolean identity");
        normalized = value.equals("true");
      }
      result.put(field.name(), normalized);
    }
    return result;
  }

  public static String quotedId(String type, Object... parts) {
    if (type.isEmpty() || parts.length == 0)
      throw new IllegalArgumentException("empty semantic identity");
    var id = new StringBuilder(type);
    for (Object part : parts) {
      String text =
          part instanceof Boolean flag
              ? (flag ? "True" : "False")
              : part instanceof Double number
                  ? CanonicalJson.stringify(number)
                  : String.valueOf(part);
      if (text.isEmpty() || part == null)
        throw new IllegalArgumentException("empty semantic identity");
      id.append(':');
      for (byte encoded : text.getBytes(StandardCharsets.UTF_8)) {
        int c = encoded & 255;
        if (c >= 'a' && c <= 'z'
            || c >= 'A' && c <= 'Z'
            || c >= '0' && c <= '9'
            || "-._~".indexOf(c) >= 0) id.append((char) c);
        else
          id.append('%')
              .append("0123456789ABCDEF".charAt(c >>> 4))
              .append("0123456789ABCDEF".charAt(c & 15));
      }
    }
    return id.toString();
  }

  public static String edgeId(String source, String type, String target) {
    return "edge:"
        + CanonicalJson.sha256(Map.of("source_id", source, "type", type, "target_id", target));
  }
}
