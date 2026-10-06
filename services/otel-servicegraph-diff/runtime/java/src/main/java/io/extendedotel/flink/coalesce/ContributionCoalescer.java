package io.extendedotel.flink.coalesce;

import io.extendedotel.flink.lifecycle.LifecyclePolicy;
import io.extendedotel.flink.model.Contribution;
import io.extendedotel.flink.serialization.ContributionTypeInformation;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Distributes and rate-limits unchanged contributor refreshes before element-keyed lifecycle work.
 */
public final class ContributionCoalescer
    extends KeyedProcessFunction<String, Contribution, Contribution> {
  public static final String UID = "graph-contribution-coalescer";
  public static final String LATEST_STATE = "coalescer-latest-contribution";
  public static final String LAST_FORWARDED_STATE = "coalescer-last-forwarded-processing-time";
  public static final String LAST_SEEN_STATE = "coalescer-last-seen-processing-time";
  public static final String PENDING_STATE = "coalescer-refresh-pending";
  public static final String TIMER_STATE = "coalescer-processing-timer";

  private final long intervalMillis;
  private final LifecyclePolicy lifecyclePolicy;

  private transient ValueState<Contribution> latest;
  private transient ValueState<Long> lastForwarded;
  private transient ValueState<Long> lastSeen;
  private transient ValueState<Boolean> pending;
  private transient ValueState<Long> timer;
  private transient Counter received;
  private transient Counter forwardedNew;
  private transient Counter forwardedChanged;
  private transient Counter forwardedUnchanged;
  private transient Counter forwardedTimerRefresh;
  private transient Counter suppressedUnchanged;
  private transient Counter ignoredOutOfOrder;

  public ContributionCoalescer(
      int intervalSeconds, int defaultTtlSeconds, Map<String, Integer> typeTtlSeconds) {
    if (intervalSeconds < 0) {
      throw new IllegalArgumentException("coalescing interval must not be negative");
    }
    intervalMillis = Math.multiplyExact((long) intervalSeconds, 1_000L);
    lifecyclePolicy = new LifecyclePolicy(defaultTtlSeconds, typeTtlSeconds);
  }

  /**
   * A length-prefixed pair avoids collisions without constraining either graph-owned identifier.
   */
  public static String key(Contribution contribution) {
    String elementId = contribution.element().id();
    return elementId.length() + ":" + elementId + contribution.contributorId();
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    var metricGroup = getRuntimeContext().getMetricGroup().addGroup("contribution_coalescer");
    received = metricGroup.counter("received_contributions");
    forwardedNew = metricGroup.counter("new_contributions_forwarded");
    forwardedChanged = metricGroup.counter("changed_contributions_forwarded");
    forwardedUnchanged = metricGroup.counter("unchanged_refreshes_forwarded");
    forwardedTimerRefresh = metricGroup.counter("timer_refreshes_forwarded");
    suppressedUnchanged = metricGroup.counter("unchanged_contributions_suppressed");
    ignoredOutOfOrder = metricGroup.counter("out_of_order_contributions_ignored");
    if (intervalMillis == 0) {
      return;
    }
    latest =
        getRuntimeContext()
            .getState(
                new ValueStateDescriptor<>(LATEST_STATE, ContributionTypeInformation.INSTANCE));
    lastForwarded =
        getRuntimeContext().getState(new ValueStateDescriptor<>(LAST_FORWARDED_STATE, Types.LONG));
    lastSeen =
        getRuntimeContext().getState(new ValueStateDescriptor<>(LAST_SEEN_STATE, Types.LONG));
    pending =
        getRuntimeContext().getState(new ValueStateDescriptor<>(PENDING_STATE, Types.BOOLEAN));
    timer = getRuntimeContext().getState(new ValueStateDescriptor<>(TIMER_STATE, Types.LONG));
  }

  @Override
  public void processElement(
      Contribution observation, Context context, Collector<Contribution> output) throws Exception {
    received.inc();
    if (intervalMillis == 0) {
      output.collect(observation);
      return;
    }
    Contribution previous = latest.value();
    if (previous != null
        && observation.observedAtUnixNano().compareTo(previous.observedAtUnixNano()) < 0) {
      ignoredOutOfOrder.inc();
      return;
    }

    long now = context.timerService().currentProcessingTime();
    latest.update(observation);
    lastSeen.update(now);
    if (previous == null) {
      output.collect(observation);
      forwardedNew.inc();
      lastForwarded.update(now);
      pending.update(false);
      replaceTimer(cleanupDeadline(now, observation), context.timerService());
      return;
    }
    if (!observation.element().equals(previous.element())) {
      output.collect(observation);
      forwardedChanged.inc();
      lastForwarded.update(now);
      pending.update(false);
      replaceTimer(cleanupDeadline(now, observation), context.timerService());
      return;
    }

    long nextRefresh = Math.addExact(lastForwarded.value(), intervalMillis);
    if (now >= nextRefresh) {
      output.collect(observation);
      forwardedUnchanged.inc();
      lastForwarded.update(now);
      pending.update(false);
      replaceTimer(cleanupDeadline(now, observation), context.timerService());
      return;
    }
    suppressedUnchanged.inc();
    pending.update(true);
    replaceTimer(nextRefresh, context.timerService());
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext context, Collector<Contribution> output)
      throws Exception {
    Long expected = timer.value();
    if (expected == null || expected != timestamp) {
      return;
    }
    timer.clear();
    Contribution current = latest.value();
    if (current == null) {
      clearState();
      return;
    }
    if (Boolean.TRUE.equals(pending.value())) {
      output.collect(current);
      forwardedTimerRefresh.inc();
      lastForwarded.update(timestamp);
      pending.update(false);
      replaceTimer(cleanupDeadline(lastSeen.value(), current), context.timerService());
      return;
    }
    long deadline = cleanupDeadline(lastSeen.value(), current);
    if (timestamp >= deadline) {
      clearState();
    } else {
      replaceTimer(deadline, context.timerService());
    }
  }

  private long cleanupDeadline(long seenAt, Contribution contribution) {
    long ttlMillis =
        Math.multiplyExact((long) lifecyclePolicy.ttlSeconds(contribution.element()), 1_000L);
    return Math.addExact(seenAt, ttlMillis);
  }

  private void replaceTimer(long deadline, TimerService timers) throws Exception {
    Long previous = timer.value();
    if (previous != null && previous == deadline) {
      return;
    }
    if (previous != null) {
      timers.deleteProcessingTimeTimer(previous);
    }
    timers.registerProcessingTimeTimer(deadline);
    timer.update(deadline);
  }

  private void clearState() throws Exception {
    latest.clear();
    lastForwarded.clear();
    lastSeen.clear();
    pending.clear();
    timer.clear();
  }
}
