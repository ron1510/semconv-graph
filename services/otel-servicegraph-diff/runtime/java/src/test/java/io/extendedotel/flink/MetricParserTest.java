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
    assertTrue(
        parsed.mutations().stream()
            .allMatch(
                contribution ->
                    contribution
                        .contributorId()
                        .equals(
                            "304cd3684aee2ba32409d769dfb4f8081d92a35134e94e9fa1bd5a56345cda27")));
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
  void incompleteTransactionDoesNotSuppressItsService() {
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
  void interactionEvidenceSuppressesSelfLoops() {
    var parsed =
        parse(
            sum(
                MetricParser.REQUEST_TOTAL,
                point(1, attr("client", "checkout"), attr("server", "checkout"))));

    assertTrue(parsed.rejections().isEmpty());
    assertEquals(
        List.of("service:checkout"),
        parsed.mutations().stream().map(c -> c.element().id()).toList());
  }

  @Test
  void discoveryCreatesTransactionAndExecutesFromOneObservation() {
    var parsed =
        parse(
            sum(
                MetricParser.DISCOVERY,
                point(
                    1,
                    attr("service.name", "worker"),
                    attr("span.name", "reconcile customers"),
                    attr("span.kind", "SPAN_KIND_INTERNAL"))));

    assertTrue(parsed.rejections().isEmpty());
    var elements = parsed.mutations().stream().map(GraphModel.Contribution::element).toList();
    String transaction = "transaction:worker:reconcile%20customers:SPAN_KIND_INTERNAL";
    assertTrue(elements.stream().anyMatch(element -> element.id().equals("service:worker")));
    assertTrue(elements.stream().anyMatch(element -> element.id().equals(transaction)));
    assertTrue(
        elements.stream()
            .anyMatch(
                element ->
                    element
                        .id()
                        .equals(
                            SemanticRegistry.edgeId("service:worker", "executes", transaction))));
  }

  @Test
  void servicegraphEvidenceCannotCreateSpanmetricsOnlyTransaction() {
    var parsed =
        parse(
            sum(
                MetricParser.REQUEST_TOTAL,
                point(
                    1,
                    attr("client", "checkout"),
                    attr("server", "payments"),
                    attr("client_span.name", "checkout"),
                    attr("client_span.kind", "SPAN_KIND_INTERNAL"))));

    assertTrue(parsed.rejections().isEmpty());
    assertTrue(
        parsed.mutations().stream()
            .map(GraphModel.Contribution::element)
            .noneMatch(element -> element.type().equals("transaction")));
  }

  @Test
  void transactionKindParticipatesInIdentityAndUnsupportedKindSkipsOnlyTransaction() {
    var internal =
        parse(
            sum(
                MetricParser.DISCOVERY,
                point(
                    1,
                    attr("service.name", "worker"),
                    attr("span.name", "consume"),
                    attr("span.kind", "SPAN_KIND_INTERNAL"))));
    var consumer =
        parse(
            sum(
                MetricParser.DISCOVERY,
                point(
                    1,
                    attr("service.name", "worker"),
                    attr("span.name", "consume"),
                    attr("span.kind", "SPAN_KIND_CONSUMER"))));
    var server =
        parse(
            sum(
                MetricParser.DISCOVERY,
                point(
                    1,
                    attr("service.name", "worker"),
                    attr("span.name", "consume"),
                    attr("span.kind", "SPAN_KIND_SERVER"))));

    var internalTransaction =
        internal.mutations().stream()
            .map(GraphModel.Contribution::element)
            .filter(element -> element.type().equals("transaction"))
            .findFirst()
            .orElseThrow();
    var consumerTransaction =
        consumer.mutations().stream()
            .map(GraphModel.Contribution::element)
            .filter(element -> element.type().equals("transaction"))
            .findFirst()
            .orElseThrow();
    assertTrue(!internalTransaction.id().equals(consumerTransaction.id()));
    assertEquals(
        List.of("service:worker"),
        server.mutations().stream().map(item -> item.element().id()).toList());
  }

  @Test
  void serverObservationCreatesAppEndpointAndExposes() {
    var parsed =
        parse(
            sum(
                MetricParser.REQUEST_TOTAL,
                point(
                    1,
                    attr("client", "checkout"),
                    attr("server", "payments"),
                    attr("server_service.namespace", "shop"),
                    attr("server_http.request.method", "GET"),
                    attr("server_http.route", "/pay"))));

    assertTrue(parsed.rejections().isEmpty());
    var elements = parsed.mutations().stream().map(GraphModel.Contribution::element).toList();
    String endpoint = "app.endpoint:payments:shop:GET:%2Fpay";
    assertTrue(elements.stream().anyMatch(element -> element.id().equals(endpoint)));
    assertTrue(
        elements.stream()
            .anyMatch(
                element ->
                    element
                        .id()
                        .equals(SemanticRegistry.edgeId("service:payments", "exposes", endpoint))));
  }

  @Test
  void connectionTypeSelectsInteractionAndUnknownSelectorKeepsObservations() {
    for (var expected :
        List.of(
            List.of("", "calls"),
            List.of("messaging_system", "publishes_to"),
            List.of("database", "queries"))) {
      var parsed =
          parse(
              sum(
                  MetricParser.REQUEST_TOTAL,
                  point(
                      1,
                      attr("client", "checkout"),
                      attr("server", "payments"),
                      attr("connection_type", expected.get(0)))));
      assertTrue(parsed.rejections().isEmpty());
      assertTrue(
          parsed.mutations().stream()
              .map(GraphModel.Contribution::element)
              .anyMatch(element -> element.type().equals(expected.get(1))));
    }

    var unknown =
        parse(
            sum(
                MetricParser.REQUEST_TOTAL,
                point(
                    1,
                    attr("client", "checkout"),
                    attr("server", "payments"),
                    attr("connection_type", "future_protocol"))));
    assertEquals(2, unknown.mutations().size());
    assertEquals("unsupported_servicegraph_connection_type", unknown.rejections().get(0).reason());
  }

  @Test
  void discoveryCreatesEtlHierarchyFromRegistryDefinitions() {
    var parsed =
        parse(
            sum(
                MetricParser.DISCOVERY,
                point(
                    1,
                    attr("service.name", "worker"),
                    attr("etl.pipeline.id", "customers"),
                    attr("etl.run.id", "run-1"),
                    attr("etl.part.run.id", "extract"))));

    assertTrue(parsed.rejections().isEmpty());
    var elements = parsed.mutations().stream().map(GraphModel.Contribution::element).toList();
    assertTrue(
        elements.stream().anyMatch(element -> element.id().equals("etl.pipeline:customers")));
    assertTrue(
        elements.stream().anyMatch(element -> element.id().equals("etl.run:customers:run-1")));
    assertTrue(
        elements.stream()
            .anyMatch(element -> element.id().equals("etl.part.run:customers:run-1:extract")));
    assertEquals(2, elements.stream().filter(element -> element.type().equals("contains")).count());
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
