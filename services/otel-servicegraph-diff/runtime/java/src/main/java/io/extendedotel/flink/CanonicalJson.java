package io.extendedotel.flink;

import com.fasterxml.jackson.core.io.schubfach.DoubleToDecimal;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Python json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False). */
public final class CanonicalJson {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature())
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final TypeReference<Map<String, Object>> OBJECT = new TypeReference<>() {};
  private static final Comparator<String> CODE_POINT_ORDER =
      (left, right) -> {
        int a = 0;
        int b = 0;
        while (a < left.length() && b < right.length()) {
          int x = left.codePointAt(a);
          int y = right.codePointAt(b);
          if (x != y) {
            return Integer.compare(x, y);
          }
          a += Character.charCount(x);
          b += Character.charCount(y);
        }
        return Integer.compare(left.length() - a, right.length() - b);
      };

  private CanonicalJson() {}

  public static int compareStrings(String left, String right) {
    return CODE_POINT_ORDER.compare(left, right);
  }

  public static Map<String, Object> readObject(String value) {
    try {
      return MAPPER.readValue(value, OBJECT);
    } catch (IOException exception) {
      throw new IllegalArgumentException("invalid JSON object", exception);
    }
  }

  public static Map<String, Object> parseObject(String value) {
    return readObject(value);
  }

  public static String stringify(Object value) {
    StringBuilder output = new StringBuilder();
    append(value, output);
    return output.toString();
  }

  public static String digest(Object value) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(stringify(value).getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 unavailable", exception);
    }
  }

  public static String sha256(Object value) {
    return digest(value);
  }

  private static void append(Object value, StringBuilder output) {
    if (value == null) {
      output.append("null");
    } else if (value instanceof String string) {
      quote(string, output);
    } else if (value instanceof Boolean bool) {
      output.append(bool);
    } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
      output.append(pythonFloat(((Number) value).doubleValue()));
    } else if (value instanceof Number number) {
      if (!(number instanceof Byte
          || number instanceof Short
          || number instanceof Integer
          || number instanceof Long
          || number instanceof BigInteger)) {
        throw new IllegalArgumentException("unsupported JSON number type");
      }
      output.append(number);
    } else if (value instanceof Map<?, ?> map) {
      List<String> keys = new ArrayList<>(map.size());
      for (Object key : map.keySet()) {
        if (!(key instanceof String string)) {
          throw new IllegalArgumentException("JSON object key must be a string");
        }
        keys.add(string);
      }
      keys.sort(CODE_POINT_ORDER);
      output.append('{');
      boolean first = true;
      for (String key : keys) {
        if (!first) {
          output.append(',');
        }
        first = false;
        quote(key, output);
        output.append(':');
        append(map.get(key), output);
      }
      output.append('}');
    } else if (value instanceof Iterable<?> values) {
      output.append('[');
      boolean first = true;
      for (Object item : values) {
        if (!first) {
          output.append(',');
        }
        first = false;
        append(item, output);
      }
      output.append(']');
    } else {
      throw new IllegalArgumentException(
          "unsupported JSON value type: " + value.getClass().getName());
    }
  }

  private static void quote(String value, StringBuilder output) {
    output.append('"');
    for (int index = 0; index < value.length(); index++) {
      char ch = value.charAt(index);
      switch (ch) {
        case '"' -> output.append("\\\"");
        case '\\' -> output.append("\\\\");
        case '\b' -> output.append("\\b");
        case '\f' -> output.append("\\f");
        case '\n' -> output.append("\\n");
        case '\r' -> output.append("\\r");
        case '\t' -> output.append("\\t");
        default -> {
          if (ch < 0x20) {
            output.append("\\u");
            String hex = Integer.toHexString(ch);
            output.append("0".repeat(4 - hex.length())).append(hex);
          } else if (Character.isHighSurrogate(ch)) {
            if (index + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(index + 1))) {
              throw new IllegalArgumentException("unpaired Unicode surrogate");
            }
            output.append(ch).append(value.charAt(++index));
          } else if (Character.isLowSurrogate(ch)) {
            throw new IllegalArgumentException("unpaired Unicode surrogate");
          } else {
            output.append(ch);
          }
        }
      }
    }
    output.append('"');
  }

  /** Schubfach supplies the decimal; only Python's formatting/one-digit choice is custom. */
  static String pythonFloat(double value) {
    if (Double.isNaN(value)) {
      return "NaN";
    }
    if (Double.isInfinite(value)) {
      return value < 0 ? "-Infinity" : "Infinity";
    }
    if (value == 0) {
      return Double.doubleToRawLongBits(value) < 0 ? "-0.0" : "0.0";
    }
    BigDecimal decimal = new BigDecimal(DoubleToDecimal.toString(value)).stripTrailingZeros();
    // Java's decimal selection permits two digits when one suffices (notably subnormals).
    if (decimal.precision() == 2) {
      BigDecimal shorter = decimal.round(new MathContext(1)).stripTrailingZeros();
      if (Double.parseDouble(shorter.toString()) == value) {
        decimal = shorter;
      }
    }
    int exponent = decimal.precision() - decimal.scale() - 1;
    if (exponent >= -4 && exponent < 16) {
      String plain = decimal.toPlainString();
      return plain.indexOf('.') < 0 ? plain + ".0" : plain;
    }
    String digits = decimal.unscaledValue().abs().toString();
    String fraction = digits.length() == 1 ? "" : "." + digits.substring(1);
    String exponentDigits = Integer.toString(Math.abs(exponent));
    return (value < 0 ? "-" : "")
        + digits.charAt(0)
        + fraction
        + "e"
        + (exponent < 0 ? "-" : "+")
        + (exponentDigits.length() == 1 ? "0" : "")
        + exponentDigits;
  }
}
