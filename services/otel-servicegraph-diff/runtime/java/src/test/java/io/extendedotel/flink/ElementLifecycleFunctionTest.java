package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.extendedotel.flink.GraphModel.Contribution;
import io.extendedotel.flink.GraphModel.Element;
import io.extendedotel.flink.GraphModel.Event;
import java.nio.file.Path;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ElementLifecycleFunctionTest {
  @TempDir Path temporary;

  @Test
  void identicalEvidenceRefreshesExpiryWithoutRepeatedPublicUpsert() throws Exception {
    Element node = Element.node("service:one", "service", Map.of("service.name", "one"));
    try (var harness = harness("refresh", null, 5)) {
      harness.setProcessingTime(1000);
      harness.processElement(new Contribution("a", 1_000_000_000L, node), 1000);
      assertEquals(1, harness.extractOutputValues().size());
      harness.getOutput().clear();
      harness.setProcessingTime(4000);
      harness.processElement(new Contribution("a", 4_000_000_000L, node), 4000);
      assertTrue(harness.extractOutputValues().isEmpty());
      harness.setProcessingTime(6000); // Conservative callback for the old deadline.
      assertTrue(harness.extractOutputValues().isEmpty());
      assertEquals(1, harness.numProcessingTimeTimers());
      harness.setProcessingTime(9000);
      assertEquals("delete", harness.extractOutputValues().get(0).operation());
      assertNull(function(harness).loadState());
    }
  }

  @Test
  void checkpointRecoveryPreservesContributorsWinnersAndIdleTimers() throws Exception {
    Element first = Element.node("service:shared", "service", Map.of("zone", "a", "version", "1"));
    Element second = Element.node("service:shared", "service", Map.of("version", "2"));
    OperatorSubtaskState checkpoint;
    try (var running = harness("before", null, 5)) {
      running.setProcessingTime(1000);
      running.processElement(new Contribution("a", 1_000_000_000L, first), 1000);
      running.setProcessingTime(2000);
      running.processElement(new Contribution("b", 2_000_000_000L, second), 2000);
      checkpoint = running.snapshot(1, 2000);
    }
    try (var restored = harness("after", checkpoint, 5)) {
      restored.getOperator().setCurrentKey(first.id());
      assertEquals(2, function(restored).loadState().contributors().size());
      restored.setProcessingTime(6000);
      Event partial = restored.extractOutputValues().get(0);
      assertEquals(Map.of("version", "2"), partial.element().attributes());
      restored.getOutput().clear();
      restored.setProcessingTime(7000);
      assertEquals("delete", restored.extractOutputValues().get(0).operation());
      assertNull(function(restored).loadState());
    }
  }

  @Test
  void typePolicyUsesShortestEdgeEndpointAndRoundsTimersConservatively() {
    ElementLifecycleFunction function =
        new ElementLifecycleFunction(300, Map.of("service", 100, "app.endpoint", 20, "calls", 40));
    assertEquals(100, function.ttlSeconds(Element.node("service:a", "service", Map.of())));
    assertEquals(
        20,
        function.ttlSeconds(
            Element.edge("edge:a", "calls", "service:a", "app.endpoint:b", Map.of())));
    assertEquals(
        7000,
        ElementLifecycleFunction.coalescedTimerMillis(
            ElementLifecycleFunction.timerMillis(6_000_000_001L)));
  }

  private KeyedOneInputStreamOperatorTestHarness<String, Contribution, Event> harness(
      String directory, OperatorSubtaskState restored, int ttl) throws Exception {
    EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
    backend.setPriorityQueueStateType(EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
    backend.setDbStoragePath(temporary.resolve(directory).toString());
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new ElementLifecycleFunction(ttl, Map.of())),
            Contribution::elementKey,
            Types.STRING);
    harness.setStateBackend(backend);
    harness.setup(new EventTypeInformation.Serializer());
    if (restored != null) harness.initializeState(restored);
    harness.open();
    return harness;
  }

  private static ElementLifecycleFunction function(
      KeyedOneInputStreamOperatorTestHarness<String, Contribution, Event> harness) {
    var operator = (KeyedProcessOperator<String, Contribution, Event>) harness.getOperator();
    return (ElementLifecycleFunction) operator.getUserFunction();
  }
}
