package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.extendedotel.flink.GraphModel.Element;
import io.extendedotel.flink.GraphModel.Snapshot;
import io.extendedotel.flink.GraphModel.State;
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.TimerService;
import org.junit.jupiter.api.Test;

final class GranularLifecycleStateTest {
  @Test
  void updatingOneOfTwoThousandSnapshotsWritesOnlyThatEntryAndKeepsCompactState() throws Exception {
    CountingMapState contributors = new CountingMapState();
    CountingValueState<GraphModel.Aggregate> aggregate = new CountingValueState<>();
    CountingValueState<AttributeWinners> winners = new CountingValueState<>();
    CountingValueState<Long> eventTimer = new CountingValueState<>();
    CountingValueState<Long> processingTimer = new CountingValueState<>();
    Map<String, ValueState<?>> values =
        Map.of(
            ElementLifecycleFunction.AGGREGATE_STATE,
            aggregate,
            ElementLifecycleFunction.WINNERS_STATE,
            winners,
            ElementLifecycleFunction.EVENT_TIMER_STATE,
            eventTimer,
            ElementLifecycleFunction.PROCESSING_TIMER_STATE,
            processingTimer);
    RuntimeContext runtime =
        (RuntimeContext)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {RuntimeContext.class},
                (proxy, method, arguments) ->
                    switch (method.getName()) {
                      case "getMapState" -> contributors;
                      case "getState" ->
                          values.get(((StateDescriptor<?, ?>) arguments[0]).getName());
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    ElementLifecycleFunction function = new ElementLifecycleFunction(86400, Map.of());
    function.setRuntimeContext(runtime);
    function.open(null);
    Element node = Element.node("service:shared", "service", Map.of());
    Map<String, Snapshot> snapshots = new LinkedHashMap<>();
    for (int index = 0; index < 2000; index++) {
      snapshots.put(
          "contributor-" + index,
          new Snapshot(
              BigInteger.valueOf(index + 1),
              BigInteger.valueOf(10_000_000_001L + index * 1_000_000L),
              20_001L + index,
              node));
    }
    State previous = new State(node.id(), snapshots, "unchanged-payload-hash");
    CountingTimers timers = new CountingTimers();
    function.persistState(null, previous);
    function.scheduleTimers(previous, timers);
    contributors.puts.clear();
    Snapshot snapshot = snapshots.get("contributor-1000");
    snapshots.put(
        "contributor-1000",
        new Snapshot(
            BigInteger.valueOf(50_000),
            snapshot.eventExpiresAtUnixNano(),
            snapshot.processingExpiresAtUnixMs(),
            snapshot.element()));
    State current = new State(node.id(), snapshots, "unchanged-payload-hash");
    function.persistState(previous, current);
    function.scheduleTimers(current, timers);
    assertEquals(List.of("contributor-1000"), contributors.puts);
    assertEquals(
        1,
        aggregate.updates,
        "refresh cannot rewrite aggregate JSON when totals/hash stay identical");
    assertEquals(List.of(11_000L), timers.eventRegistrations);
    assertEquals(List.of(21_000L), timers.processingRegistrations);
    assertEquals(1, eventTimer.updates);
    assertEquals(1, processingTimer.updates);
    assertEquals(
        current,
        function.loadState(),
        "granular JSON round trip reconstructs an ephemeral merge snapshot");
    contributors.scans = 0;
    contributors.reads = 0;
    contributors.puts.clear();
    function.observe(
        new GraphModel.Contribution("contributor-1000", 60_000L, node),
        timers,
        new org.apache.flink.util.Collector<GraphModel.Event>() {
          @Override
          public void collect(GraphModel.Event event) {}

          @Override
          public void close() {}
        });
    assertEquals(0, contributors.scans, "a routine refresh cannot iterate retained contributors");
    assertEquals(
        1, contributors.reads, "only the refreshed snapshot is read for an empty-attribute node");
    assertEquals(List.of("contributor-1000"), contributors.puts);
    assertEquals(2000, winners.value().contributorCount());
  }

  private static final class CountingMapState implements MapState<String, Snapshot> {
    private final Map<String, Snapshot> entries = new LinkedHashMap<>();
    private int scans;
    private int reads;
    private final List<String> puts = new ArrayList<>();

    @Override
    public Snapshot get(String key) {
      reads++;
      return entries.get(key);
    }

    @Override
    public void put(String key, Snapshot value) {
      entries.put(key, value);
      puts.add(key);
    }

    @Override
    public void putAll(Map<String, Snapshot> map) {
      map.forEach(this::put);
    }

    @Override
    public void remove(String key) {
      entries.remove(key);
    }

    @Override
    public boolean contains(String key) {
      return entries.containsKey(key);
    }

    @Override
    public Iterable<Map.Entry<String, Snapshot>> entries() {
      scans++;
      return entries.entrySet();
    }

    @Override
    public Iterable<String> keys() {
      return entries.keySet();
    }

    @Override
    public Iterable<Snapshot> values() {
      return entries.values();
    }

    @Override
    public Iterator<Map.Entry<String, Snapshot>> iterator() {
      return entries.entrySet().iterator();
    }

    @Override
    public boolean isEmpty() {
      return entries.isEmpty();
    }

    @Override
    public void clear() {
      entries.clear();
    }
  }

  private static final class CountingValueState<T> implements ValueState<T> {
    private T value;
    private int updates;

    @Override
    public T value() {
      return value;
    }

    @Override
    public void update(T next) {
      value = next;
      updates++;
    }

    @Override
    public void clear() {
      value = null;
    }
  }

  private static final class CountingTimers implements TimerService {
    private final List<Long> eventRegistrations = new ArrayList<>();
    private final List<Long> processingRegistrations = new ArrayList<>();

    @Override
    public long currentProcessingTime() {
      return 0;
    }

    @Override
    public long currentWatermark() {
      return 0;
    }

    @Override
    public void registerProcessingTimeTimer(long timestamp) {
      processingRegistrations.add(timestamp);
    }

    @Override
    public void registerEventTimeTimer(long timestamp) {
      eventRegistrations.add(timestamp);
    }

    @Override
    public void deleteProcessingTimeTimer(long timestamp) {
      throw new AssertionError("timer should be unchanged");
    }

    @Override
    public void deleteEventTimeTimer(long timestamp) {
      throw new AssertionError("timer should be unchanged");
    }
  }
}
