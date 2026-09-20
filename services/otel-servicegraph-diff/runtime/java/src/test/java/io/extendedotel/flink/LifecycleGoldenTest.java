package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.extendedotel.flink.GraphModel.Contribution;
import io.extendedotel.flink.GraphModel.Element;
import java.math.BigInteger;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class LifecycleGoldenTest {
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
