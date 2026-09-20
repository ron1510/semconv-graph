package io.extendedotel.flink;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

/** Counters count every rejection; logs sample each finite reason at most once per minute. */
final class RejectionLog {
  private static final long INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
  private final Logger logger;
  private final Map<String, Long> lastLogged = new HashMap<>();

  RejectionLog(Logger logger) {
    this.logger = logger;
  }

  void report(MetricParser.Rejection rejection) {
    long now = System.nanoTime();
    Long previous = lastLogged.get(rejection.reason());
    if (previous == null || now - previous >= INTERVAL_NANOS) {
      lastLogged.put(rejection.reason(), now);
      logger.warn(
          "Rejected telemetry: reason={} detail={}",
          rejection.reason(),
          MetricParser.boundedDetail(rejection.detail()));
    }
  }
}
