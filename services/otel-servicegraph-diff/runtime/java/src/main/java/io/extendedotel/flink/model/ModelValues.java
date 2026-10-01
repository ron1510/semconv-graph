package io.extendedotel.flink.model;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Shared decoding and immutable-value rules for graph domain records. */
final class ModelValues {
  private ModelValues() {}

  static Map<String, Object> object(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      throw new IllegalArgumentException("expected JSON object");
    }
    Map<String, Object> result = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (!(entry.getKey() instanceof String key)) {
        throw new IllegalArgumentException("expected string object key");
      }
      result.put(key, entry.getValue());
    }
    return result;
  }

  static BigInteger exactInteger(Object value) {
    if (value instanceof BigInteger integer) return integer;
    if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long) {
      return BigInteger.valueOf(((Number) value).longValue());
    }
    throw new IllegalArgumentException("expected exact integer");
  }

  static long exactLong(Object value) {
    return exactInteger(value).longValueExact();
  }

  static BigInteger optionalInteger(Object value) {
    return value == null ? null : exactInteger(value);
  }

  static Long optionalLong(Object value) {
    return value == null ? null : exactLong(value);
  }

  static String string(Map<String, Object> value, String name) {
    if (!(value.get(name) instanceof String string)) {
      throw new IllegalArgumentException("expected string field " + name);
    }
    return string;
  }

  static void positiveTimestamp(BigInteger value) {
    if (Objects.requireNonNull(value).signum() <= 0) {
      throw new IllegalArgumentException("observation nanoseconds must be positive");
    }
  }

  static String required(String value) {
    if (value == null || value.strip().isEmpty()) {
      throw new IllegalArgumentException("expected nonempty string");
    }
    return value.strip();
  }

  static Object jsonValue(Object value) {
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

  static Map<String, Object> immutableObject(Map<String, Object> value) {
    Map<String, Object> result = new LinkedHashMap<>();
    new TreeMap<>(value).forEach((name, item) -> result.put(name, immutable(item)));
    return Collections.unmodifiableMap(result);
  }

  private static Object immutable(Object value) {
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> typed = new LinkedHashMap<>();
      map.forEach((key, item) -> typed.put((String) key, item));
      return immutableObject(typed);
    }
    if (value instanceof List<?> list) return list.stream().map(ModelValues::immutable).toList();
    return value;
  }
}
