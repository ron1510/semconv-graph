package io.extendedotel.flink.coalesce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.extendedotel.flink.model.Contribution;
import io.extendedotel.flink.model.Element;
import io.extendedotel.flink.serialization.ContributionTypeInformation;
import java.nio.file.Path;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ContributionCoalescerTest {
  @TempDir Path temporary;

  @Test
  void unchangedEvidenceForwardsLatestObservationOncePerInterval() throws Exception {
    Element node = node("one", "1");
    try (var harness = harness("refresh", null, 5, 20)) {
      harness.setProcessingTime(1_000);
      harness.processElement(new Contribution("a", 1_000_000_000L, node), 1_000);
      assertEquals(1, harness.extractOutputValues().size());
      harness.getOutput().clear();

      harness.setProcessingTime(2_000);
      harness.processElement(new Contribution("a", 2_000_000_000L, node), 2_000);
      harness.setProcessingTime(3_000);
      harness.processElement(new Contribution("a", 3_000_000_000L, node), 3_000);
      assertTrue(harness.extractOutputValues().isEmpty());
      assertEquals(1, harness.numProcessingTimeTimers());

      harness.setProcessingTime(6_000);
      var refreshes = harness.extractOutputValues();
      assertEquals(1, refreshes.size());
      assertEquals(3_000_000_000L, refreshes.get(0).observedAtUnixNano().longValueExact());
      assertEquals(1, harness.numProcessingTimeTimers());
    }
  }

  @Test
  void changesAreImmediateAndInvalidatePendingRefreshes() throws Exception {
    Element first = node("one", "1");
    Element second = node("one", "2");
    try (var harness = harness("changes", null, 5, 20)) {
      harness.setProcessingTime(1_000);
      harness.processElement(new Contribution("a", 1_000_000_000L, first), 1_000);
      harness.getOutput().clear();
      harness.setProcessingTime(2_000);
      harness.processElement(new Contribution("a", 2_000_000_000L, first), 2_000);

      harness.setProcessingTime(3_000);
      harness.processElement(new Contribution("a", 3_000_000_000L, second), 3_000);
      assertEquals(second, harness.extractOutputValues().get(0).element());
      harness.getOutput().clear();
      harness.setProcessingTime(6_000);
      assertTrue(harness.extractOutputValues().isEmpty());

      harness.setProcessingTime(7_000);
      harness.processElement(new Contribution("a", 7_000_000_000L, first), 7_000);
      assertEquals(first, harness.extractOutputValues().get(0).element());
    }
  }

  @Test
  void olderObservationsAreIgnoredAndImmediateRefreshAfterIntervalIsForwarded() throws Exception {
    Element node = node("one", "1");
    try (var harness = harness("ordering", null, 5, 20)) {
      harness.setProcessingTime(1_000);
      harness.processElement(new Contribution("a", 2_000_000_000L, node), 2_000);
      harness.getOutput().clear();
      harness.setProcessingTime(2_000);
      harness.processElement(new Contribution("a", 1_000_000_000L, node), 1_000);
      assertTrue(harness.extractOutputValues().isEmpty());

      harness.setProcessingTime(6_000);
      harness.processElement(new Contribution("a", 6_000_000_000L, node), 6_000);
      assertEquals(1, harness.extractOutputValues().size());
    }
  }

  @Test
  void contributorsAndElementsUseIndependentCollisionFreeKeys() {
    Contribution first = new Contribution("bc", 1, Element.node("a", "service", Map.of()));
    Contribution second = new Contribution("c", 1, Element.node("ab", "service", Map.of()));
    Contribution third = new Contribution("other", 1, first.element());
    assertNotEquals(ContributionCoalescer.key(first), ContributionCoalescer.key(second));
    assertNotEquals(ContributionCoalescer.key(first), ContributionCoalescer.key(third));
  }

  @Test
  void differentContributorAndElementPairsHaveIndependentState() throws Exception {
    Element first = node("one", "1");
    Element second = node("two", "1");
    try (var harness = harness("independent", null, 5, 20)) {
      harness.setProcessingTime(1_000);
      harness.processElement(new Contribution("a", 1_000_000_000L, first), 1_000);
      harness.processElement(new Contribution("b", 1_000_000_000L, first), 1_000);
      harness.processElement(new Contribution("a", 1_000_000_000L, second), 1_000);
      assertEquals(3, harness.extractOutputValues().size());
      assertEquals(3, harness.numProcessingTimeTimers());
      harness.getOutput().clear();

      harness.setProcessingTime(2_000);
      harness.processElement(new Contribution("a", 2_000_000_000L, first), 2_000);
      harness.processElement(new Contribution("b", 2_000_000_000L, first), 2_000);
      harness.processElement(new Contribution("a", 2_000_000_000L, second), 2_000);
      assertTrue(harness.extractOutputValues().isEmpty());
      assertEquals(3, harness.numProcessingTimeTimers());
    }
  }

  @Test
  void cleanupIsSilentAndTheNextObservationIsNewAgain() throws Exception {
    Element node = node("one", "1");
    try (var harness = harness("cleanup", null, 5, 10)) {
      harness.setProcessingTime(1_000);
      harness.processElement(new Contribution("a", 1_000_000_000L, node), 1_000);
      harness.getOutput().clear();
      harness.setProcessingTime(11_000);
      assertTrue(harness.extractOutputValues().isEmpty());
      assertEquals(0, harness.numProcessingTimeTimers());

      harness.setProcessingTime(12_000);
      harness.processElement(new Contribution("a", 12_000_000_000L, node), 12_000);
      assertEquals(1, harness.extractOutputValues().size());
    }
  }

  @Test
  void zeroIntervalIsStatelessPassThrough() throws Exception {
    Element node = node("one", "1");
    try (var harness = harness("disabled", null, 0, 10)) {
      harness.setProcessingTime(1_000);
      harness.processElement(new Contribution("a", 1_000_000_000L, node), 1_000);
      harness.processElement(new Contribution("a", 2_000_000_000L, node), 2_000);
      assertEquals(2, harness.extractOutputValues().size());
      assertEquals(0, harness.numProcessingTimeTimers());
    }
  }

  @Test
  void rocksDbRecoveryPreservesPendingLatestRefreshAndTimer() throws Exception {
    Element node = node("one", "1");
    OperatorSubtaskState checkpoint;
    try (var running = harness("before", null, 5, 20)) {
      running.setProcessingTime(1_000);
      running.processElement(new Contribution("a", 1_000_000_000L, node), 1_000);
      running.setProcessingTime(2_000);
      running.processElement(new Contribution("a", 2_000_000_000L, node), 2_000);
      running.setProcessingTime(3_000);
      running.processElement(new Contribution("a", 3_000_000_000L, node), 3_000);
      checkpoint = running.snapshot(1, 3_000);
    }
    try (var restored = harness("after", checkpoint, 5, 20)) {
      restored.getOutput().clear();
      restored.setProcessingTime(6_000);
      var output = restored.extractOutputValues();
      assertEquals(1, output.size());
      assertEquals(3_000_000_000L, output.get(0).observedAtUnixNano().longValueExact());
    }
  }

  private KeyedOneInputStreamOperatorTestHarness<String, Contribution, Contribution> harness(
      String directory, OperatorSubtaskState restored, int interval, int ttl) throws Exception {
    EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
    backend.setPriorityQueueStateType(EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
    backend.setDbStoragePath(temporary.resolve(directory).toString());
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new ContributionCoalescer(interval, ttl, Map.of())),
            ContributionCoalescer::key,
            Types.STRING);
    harness.setStateBackend(backend);
    harness.setup(new ContributionTypeInformation.Serializer());
    if (restored != null) {
      harness.initializeState(restored);
    }
    harness.open();
    return harness;
  }

  private static Element node(String name, String version) {
    return Element.node(
        "service:" + name, "service", Map.of("service.name", name, "service.version", version));
  }
}
