package io.extendedotel.flink;

import io.extendedotel.flink.GraphModel.Aggregate;
import io.extendedotel.flink.GraphModel.Contribution;
import io.extendedotel.flink.GraphModel.Event;
import io.extendedotel.flink.GraphModel.Result;
import io.extendedotel.flink.GraphModel.Snapshot;
import io.extendedotel.flink.GraphModel.State;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Sole lifecycle owner: granular contributor state, compact totals, two coalesced timers. */
public final class ElementLifecycleFunction
    extends KeyedProcessFunction<String, Contribution, Event> {
  public static final String UID = "graph-java-v1-element-lifecycle";
  public static final String CONTRIBUTORS_STATE = "graph-java-element-contributors-v1";
  public static final String AGGREGATE_STATE = "graph-java-element-aggregate-v1";
  public static final String EVENT_TIMER_STATE = "graph-java-element-next-event-timer-v1";
  public static final String PROCESSING_TIMER_STATE = "graph-java-element-next-processing-timer-v1";

  public static final String WINNERS_STATE = "graph-java-element-attribute-winners-v1";

  private final LifecyclePolicy policy;
  private transient MapState<String, Snapshot> contributors;
  private transient ValueState<Aggregate> aggregate;
  private transient ValueState<AttributeWinners> winners;
  private transient ValueState<Long> eventTimer;
  private transient ValueState<Long> processingTimer;

  public ElementLifecycleFunction(int defaultTtlSeconds, Map<String, Integer> typeTtlSeconds) {
    policy = new LifecyclePolicy(defaultTtlSeconds, typeTtlSeconds);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    contributors =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    CONTRIBUTORS_STATE, StringSerializer.INSTANCE, StateSerializer.contributor()));
    aggregate =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>(AGGREGATE_STATE, StateSerializer.aggregate()));
    winners =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>(WINNERS_STATE, StateSerializer.winners()));
    eventTimer =
        getRuntimeContext().getState(new ValueStateDescriptor<>(EVENT_TIMER_STATE, Types.LONG));
    processingTimer =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>(PROCESSING_TIMER_STATE, Types.LONG));
  }

  @Override
  public void processElement(Contribution value, Context context, Collector<Event> output)
      throws Exception {
    TimerService timers = context.timerService();
    if (observe(value, timers, output)) {
      return;
    }
    State previous = loadState();
    Result result =
        GraphLifecycle.apply(
            previous,
            value,
            ttlSeconds(value.element()),
            BigInteger.valueOf(Math.max(timers.currentWatermark(), 0L))
                .multiply(BigInteger.valueOf(1_000_000L)),
            timers.currentProcessingTime(),
            System.currentTimeMillis());
    update(previous, result, timers, output);
  }

  /** The common refresh path never iterates contributor MapState. */
  boolean observe(GraphModel.Contribution observation, TimerService timers, Collector<Event> output)
      throws Exception {
    Snapshot existing = snapshot(observation.contributorId());
    if (existing != null
        && observation.observedAtUnixNano().compareTo(existing.observedAtUnixNano()) < 0) {
      return true;
    }
    AttributeWinners previousIndex = winners.value();
    AttributeWinners index;
    String previousHash;
    if (previousIndex == null) {
      State legacy = loadState();
      index = legacy == null ? null : AttributeWinners.rebuild(legacy);
      previousHash = legacy == null ? null : legacy.lastPayloadHash();
    } else {
      index = previousIndex;
      previousHash = aggregate.value().lastPayloadHash();
    }
    if (index != null && index.losesAttribute(observation.contributorId(), observation.element())) {
      return false; // A removed winner needs a replacement from the remaining contributors.
    }
    Snapshot nextSnapshot =
        policy.snapshot(
            observation,
            BigInteger.valueOf(Math.max(timers.currentWatermark(), 0L))
                .multiply(BigInteger.valueOf(1_000_000L)),
            timers.currentProcessingTime());
    if (index == null) {
      Map<String, Snapshot> initial = Map.of(observation.contributorId(), nextSnapshot);
      index = AttributeWinners.rebuild(new State(observation.element().id(), initial, ""));
    } else {
      Map<String, Snapshot> reads = new LinkedHashMap<>();
      index =
          index.observe(
              observation.contributorId(),
              nextSnapshot,
              existing != null,
              id -> {
                Snapshot cached = reads.get(id);
                if (cached == null) {
                  cached = snapshot(id);
                  if (cached == null)
                    throw new IllegalStateException("missing attribute owner " + id);
                  reads.put(id, cached);
                }
                return cached;
              });
    }
    String hash =
        previousIndex != null && index.element().equals(previousIndex.element())
            ? previousHash
            : GraphLifecycle.payloadHash(index.element());
    contributors.put(observation.contributorId(), nextSnapshot);
    if (!index.equals(previousIndex)) winners.update(index);
    Aggregate compact = new Aggregate(index.element().id(), hash);
    if (!compact.equals(aggregate.value())) aggregate.update(compact);
    advanceTimer(
        eventTimer,
        nextSnapshot.eventExpiresAtUnixNano() == null
            ? null
            : coalescedTimerMillis(timerMillis(nextSnapshot.eventExpiresAtUnixNano())),
        timers,
        true);
    advanceTimer(
        processingTimer,
        nextSnapshot.processingExpiresAtUnixMs() == null
            ? null
            : coalescedTimerMillis(nextSnapshot.processingExpiresAtUnixMs()),
        timers,
        false);
    if (!Objects.equals(previousHash, hash)) {
      output.collect(
          new GraphModel.Upsert(
              GraphLifecycle.eventId(
                  "upsert", index.element().id(), observation.observedAtUnixNano(), hash),
              observation.observedAtUnixNano(),
              System.currentTimeMillis(),
              hash,
              index.element()));
    }
    return true;
  }

  private Snapshot snapshot(String id) throws Exception {
    return contributors.get(id);
  }

  /** Keep an earlier wake-up on refresh; only timer callbacks need to discover a later minimum. */
  private static void advanceTimer(
      ValueState<Long> state, Long deadline, TimerService timers, boolean eventTime)
      throws Exception {
    Long previous = state.value();
    if (deadline != null && (previous == null || deadline < previous)) {
      replaceTimer(state, deadline, timers, eventTime);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext context, Collector<Event> output)
      throws Exception {
    if (context.timeDomain() == TimeDomain.PROCESSING_TIME) processingTimer.clear();
    else eventTimer.clear();
    State previous = loadState();
    if (previous == null) {
      return;
    }
    Result result =
        context.timeDomain() == TimeDomain.PROCESSING_TIME
            ? GraphLifecycle.expire(
                previous,
                GraphLifecycle.ExpiryClock.PROCESSING_TIME,
                timestamp,
                System.currentTimeMillis())
            : GraphLifecycle.expire(
                previous,
                GraphLifecycle.ExpiryClock.EVENT_TIME,
                BigInteger.valueOf(timestamp).multiply(BigInteger.valueOf(1_000_000L)),
                System.currentTimeMillis());
    if (result.state() == previous) {
      scheduleTimers(previous, context.timerService());
    } else {
      update(previous, result, context.timerService(), output);
    }
  }

  private void update(State previous, Result result, TimerService timers, Collector<Event> output)
      throws Exception {
    if (result.state() == previous) {
      return;
    }
    persistState(previous, result.state());
    scheduleTimers(result.state(), timers);
    if (result.event() != null) {
      output.collect(result.event());
    }
  }

  State loadState() throws Exception {
    Aggregate compact = aggregate.value();
    Map<String, Snapshot> snapshots = new LinkedHashMap<>();
    for (Map.Entry<String, Snapshot> entry : contributors.entries()) {
      snapshots.put(entry.getKey(), entry.getValue());
    }
    if (compact == null) {
      if (!snapshots.isEmpty()) {
        throw new IllegalStateException("contributors exist without aggregate state");
      }
      return null;
    }
    if (snapshots.isEmpty()) {
      throw new IllegalStateException("aggregate exists without contributors");
    }
    return new State(compact.elementId(), snapshots, compact.lastPayloadHash());
  }

  void persistState(State previous, State current) throws Exception {
    if (current == null) {
      contributors.clear();
      aggregate.clear();
      if (winners != null) winners.clear();
      return;
    }
    if (winners != null) winners.update(AttributeWinners.rebuild(current));
    Map<String, Snapshot> previousSnapshots = previous == null ? Map.of() : previous.contributors();
    for (String id : previousSnapshots.keySet()) {
      if (!current.contributors().containsKey(id)) {
        contributors.remove(id);
      }
    }
    for (Map.Entry<String, Snapshot> entry : current.contributors().entrySet()) {
      if (!entry.getValue().equals(previousSnapshots.get(entry.getKey()))) {
        contributors.put(entry.getKey(), entry.getValue());
      }
    }
    if (previous == null || !current.aggregate().equals(previous.aggregate())) {
      aggregate.update(current.aggregate());
    }
  }

  void scheduleTimers(State state, TimerService timers) throws Exception {
    Long nextEvent = null;
    Long nextProcessing = null;
    if (state != null) {
      for (Snapshot snapshot : state.contributors().values()) {
        if (snapshot.eventExpiresAtUnixNano() != null) {
          long candidate = coalescedTimerMillis(timerMillis(snapshot.eventExpiresAtUnixNano()));
          nextEvent = nextEvent == null ? candidate : Math.min(nextEvent, candidate);
        }
        if (snapshot.processingExpiresAtUnixMs() != null) {
          long candidate = coalescedTimerMillis(snapshot.processingExpiresAtUnixMs());
          nextProcessing = nextProcessing == null ? candidate : Math.min(nextProcessing, candidate);
        }
      }
    }
    replaceTimer(eventTimer, nextEvent, timers, true);
    replaceTimer(processingTimer, nextProcessing, timers, false);
  }

  private static void replaceTimer(
      ValueState<Long> state, Long deadline, TimerService timers, boolean eventTime)
      throws Exception {
    Long previous = state.value();
    if (Objects.equals(previous, deadline)) {
      return;
    }
    if (previous != null) {
      if (eventTime) {
        timers.deleteEventTimeTimer(previous);
      } else {
        timers.deleteProcessingTimeTimer(previous);
      }
    }
    if (deadline == null) {
      state.clear();
    } else {
      if (eventTime) {
        timers.registerEventTimeTimer(deadline);
      } else {
        timers.registerProcessingTimeTimer(deadline);
      }
      state.update(deadline);
    }
  }

  int ttlSeconds(GraphModel.Element element) {
    return policy.ttlSeconds(element);
  }

  static long timerMillis(long unixNano) {
    return timerMillis(BigInteger.valueOf(unixNano));
  }

  static long timerMillis(BigInteger unixNano) {
    return unixNano
        .add(BigInteger.valueOf(999_999L))
        .divide(BigInteger.valueOf(1_000_000L))
        .longValueExact();
  }

  static long coalescedTimerMillis(long unixMilli) {
    return Math.multiplyExact(unixMilli / 1_000L + (unixMilli % 1_000L == 0 ? 0 : 1), 1_000L);
  }
}
