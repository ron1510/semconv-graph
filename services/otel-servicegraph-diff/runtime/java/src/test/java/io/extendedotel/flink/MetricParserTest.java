package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import java.util.List;
import org.junit.jupiter.api.Test;

class MetricParserTest {
  private static final long OBSERVED = 1_700_000_000_123_456_789L;

  @Test
  void interactionEvidencePreservesNodesRelationshipIdsAndAttributesWithoutMagnitude() {
    var parsed =
        MetricParser.parse(
            request(
                    sum(
                        MetricParser.REQUEST_TOTAL,
                        point(
                            37,
                            attr("client", "checkout"),
                            attr("server", "payments"),
                            attr("client_service.version", "1.2.3"))))
                .toByteArray());

    assertTrue(parsed.rejections().isEmpty());
    assertEquals(3, parsed.mutations().size());
    var elements = parsed.mutations().stream().map(GraphModel.Contribution::element).toList();
    assertTrue(elements.stream().anyMatch(e -> e.id().equals("service:checkout")));
    assertTrue(elements.stream().anyMatch(e -> e.id().equals("service:payments")));
    String edgeId = SemanticRegistry.edgeId("service:checkout", "calls", "service:payments");
    assertTrue(elements.stream().anyMatch(e -> e.id().equals(edgeId)));
    assertTrue(elements.stream().allMatch(e -> !e.toMap().containsKey("metrics")));
    assertEquals(
        "1.2.3",
        elements.stream()
            .filter(e -> e.id().equals("service:checkout"))
            .findFirst()
            .orElseThrow()
            .attributes()
            .get("service.version"));
  }

  @Test
  void discoveryEvidenceProducesTheSameSemanticNode() {
    var parsed =
        MetricParser.parse(
            request(
                    sum(
                        MetricParser.DISCOVERY,
                        point(
                            1,
                            attr("service.name", "worker"),
                            attr("span.kind", "SPAN_KIND_INTERNAL"))))
                .toByteArray());

    assertTrue(parsed.rejections().isEmpty());
    assertEquals(
        List.of("service:worker"), parsed.mutations().stream().map(c -> c.element().id()).toList());
  }

  @Test
  void malformedWrongTypeAndWrongTemporalityAreRejected() {
    assertEquals(
        "invalid_otlp_protobuf",
        MetricParser.parse(new byte[] {0x0a, 0x05, 0x01}).rejections().get(0).reason());
    var gauge =
        Metric.newBuilder()
            .setName(MetricParser.REQUEST_TOTAL)
            .setGauge(Gauge.newBuilder())
            .build();
    assertEquals("invalid_servicegraph_metric_type", parse(gauge).rejections().get(0).reason());
    var cumulative =
        Metric.newBuilder()
            .setName(MetricParser.REQUEST_TOTAL)
            .setSum(
                Sum.newBuilder()
                    .setAggregationTemporality(
                        AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                    .addDataPoints(point(1, attr("client", "a"), attr("server", "b"))))
            .build();
    assertEquals(
        "invalid_servicegraph_temporality", parse(cumulative).rejections().get(0).reason());
  }

  @Test
  void onlyPositiveFiniteEvidenceIsAcceptedAndFailureMetricIsIgnored() {
    for (double value : List.of(0.0, -1.0, Double.NaN, Double.POSITIVE_INFINITY)) {
      var parsed =
          parse(
              sum(
                  MetricParser.REQUEST_TOTAL,
                  point(value, attr("client", "a"), attr("server", "b"))));
      assertTrue(parsed.mutations().isEmpty());
      assertEquals("invalid_servicegraph_datapoint", parsed.rejections().get(0).reason());
    }
    var ignored =
        parse(
            sum(
                "traces_service_graph_request_failed_total",
                point(1, attr("client", "a"), attr("server", "b"))));
    assertTrue(ignored.mutations().isEmpty());
    assertTrue(ignored.rejections().isEmpty());
  }

  private static MetricParser.Parsed parse(Metric metric) {
    return MetricParser.parse(request(metric).toByteArray());
  }

  private static ExportMetricsServiceRequest request(Metric metric) {
    return ExportMetricsServiceRequest.newBuilder()
        .addResourceMetrics(
            ResourceMetrics.newBuilder()
                .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(metric)))
        .build();
  }

  private static Metric sum(String name, NumberDataPoint point) {
    return Metric.newBuilder()
        .setName(name)
        .setSum(
            Sum.newBuilder()
                .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
                .addDataPoints(point))
        .build();
  }

  private static NumberDataPoint point(long value, KeyValue... attributes) {
    return NumberDataPoint.newBuilder()
        .setTimeUnixNano(OBSERVED)
        .setAsInt(value)
        .addAllAttributes(List.of(attributes))
        .build();
  }

  private static NumberDataPoint point(double value, KeyValue... attributes) {
    return NumberDataPoint.newBuilder()
        .setTimeUnixNano(OBSERVED)
        .setAsDouble(value)
        .addAllAttributes(List.of(attributes))
        .build();
  }

  private static KeyValue attr(String key, String value) {
    return KeyValue.newBuilder()
        .setKey(key)
        .setValue(AnyValue.newBuilder().setStringValue(value))
        .build();
  }
}
