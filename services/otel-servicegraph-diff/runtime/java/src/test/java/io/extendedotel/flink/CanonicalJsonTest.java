package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class CanonicalJsonTest {
  @Test
  void byteForBytePythonUnicodeNumbersAndHashes() throws Exception {
    Map<String, Object> fixture;
    try (var input = getClass().getResourceAsStream("/canonical-golden.json")) {
      if (input == null) throw new IllegalStateException("missing canonical fixture");
      fixture = CanonicalJson.readObject(new String(input.readAllBytes(), StandardCharsets.UTF_8));
    }
    int count = 0;
    for (Object raw : (List<?>) fixture.get("cases")) {
      Map<String, Object> example = GraphModel.object(raw);
      Object value = example.get("value");
      assertEquals(
          example.get("json"), CanonicalJson.stringify(value), "Python encoding case " + count);
      assertEquals(
          example.get("sha256"), CanonicalJson.digest(value), "Python digest case " + count);
      count++;
    }
    assertTrue(count > 3800);
  }

  @Test
  void supportsPythonNonfiniteAttributeConstantsAndRejectsBrokenUnicode() {
    assertEquals("NaN", CanonicalJson.stringify(Double.NaN));
    assertEquals("Infinity", CanonicalJson.stringify(Double.POSITIVE_INFINITY));
    assertEquals("-Infinity", CanonicalJson.stringify(Double.NEGATIVE_INFINITY));
    assertTrue(
        Double.isNaN(
            ((Number) CanonicalJson.readObject("{\"value\":NaN}").get("value")).doubleValue()));
    assertThrows(IllegalArgumentException.class, () -> CanonicalJson.digest("\ud800"));
  }
}
