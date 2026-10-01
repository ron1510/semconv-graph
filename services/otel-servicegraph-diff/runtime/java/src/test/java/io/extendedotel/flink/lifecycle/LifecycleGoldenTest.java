package io.extendedotel.flink.lifecycle;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.extendedotel.flink.model.Contribution;
import io.extendedotel.flink.model.Element;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class LifecycleGoldenTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  @SuppressWarnings("unchecked")
  void recursivelyOrdersAttributesBeforeDefaultJacksonHashing() throws Exception {
    var nestedFirst = new LinkedHashMap<String, Object>();
    nestedFirst.put("z", 1);
    nestedFirst.put("a", 2);
    var first = new LinkedHashMap<String, Object>();
    first.put("z", 1);
    first.put("nested", nestedFirst);
    first.put("a", 2);

    var nestedSecond = new LinkedHashMap<String, Object>();
    nestedSecond.put("a", 2);
    nestedSecond.put("z", 1);
    var second = new LinkedHashMap<String, Object>();
    second.put("a", 2);
    second.put("nested", nestedSecond);
    second.put("z", 1);

    Element left = Element.node("service:one", "service", first);
    Element right = Element.node("service:one", "service", second);

    assertEquals(left, right);
    assertEquals(List.of("a", "nested", "z"), new ArrayList<>(left.attributes().keySet()));
    assertEquals(
        List.of("a", "z"),
        new ArrayList<>(((Map<String, Object>) left.attributes().get("nested")).keySet()));
    assertArrayEquals(JSON.writeValueAsBytes(left.toMap()), JSON.writeValueAsBytes(right.toMap()));
    String payloadHash = GraphLifecycle.payloadHash(left);
    assertEquals(payloadHash, GraphLifecycle.payloadHash(right));
    assertEquals("8cda6b52db893c36b9f1666e8598ece8e904b8201485f7d8c9732a54b3199659", payloadHash);
    assertEquals(
        "bdf8240de5f8f6ed1e714375257d720d44b842305181ec9faa8d2a87edbc8457",
        GraphLifecycle.eventId("upsert", left.id(), BigInteger.ONE, payloadHash));
  }

  @Test
  void repeatedEvidenceRefreshesDeadlineWithoutAnotherUpsert() {
    Element node = Element.node("service:one", "service", Map.of("version", "1"));
    var first = GraphLifecycle.apply(null, new Contribution("a", 1, node), 5, 0, 100, 101);
    var refresh =
        GraphLifecycle.apply(
            first.state(), new Contribution("a", 2_000_000_000L, node), 5, 0, 200, 201);

    assertNull(refresh.event());
    assertEquals(
        BigInteger.valueOf(7_000_000_000L),
        refresh.state().contributors().get("a").eventExpiresAtUnixNano());
    assertEquals(5200L, refresh.state().contributors().get("a").processingExpiresAtUnixMs());
  }

  @Test
  void multipleContributorsMergeDeterministicallyThenPartiallyAndFinallyExpire() {
    Element old = Element.node("service:one", "service", Map.of("zone", "a", "version", "1"));
    Element newer = Element.node("service:one", "service", Map.of("version", "2"));
    var first = GraphLifecycle.apply(null, new Contribution("a", 1, old), 5, 0, 100, 101);
    var second =
        GraphLifecycle.apply(
            first.state(), new Contribution("b", 2_000_000_000L, newer), 5, 0, 2000, 102);
    assertEquals(Map.of("zone", "a", "version", "2"), second.event().element().attributes());

    var partial =
        GraphLifecycle.expire(
            second.state(), GraphLifecycle.ExpiryClock.PROCESSING_TIME, 5100, 103);
    assertEquals(Map.of("version", "2"), partial.event().element().attributes());
    var deleted =
        GraphLifecycle.expire(
            partial.state(), GraphLifecycle.ExpiryClock.PROCESSING_TIME, 7000, 104);
    assertEquals("delete", deleted.event().operation());
    assertNull(deleted.state());
  }

  @Test
  void rejectsConflictingElementIdentityAndNonpositiveTtl() {
    Element node = Element.node("service:one", "service", Map.of());
    var active = GraphLifecycle.apply(null, new Contribution("a", 1, node), 5, 0, 0, 0).state();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GraphLifecycle.apply(
                active,
                new Contribution("b", 2, Element.node("service:one", "host", Map.of())),
                5,
                0,
                0,
                0));
    assertThrows(
        IllegalArgumentException.class,
        () -> GraphLifecycle.apply(null, new Contribution("a", 1, node), 0, 0, 0, 0));
  }
}
