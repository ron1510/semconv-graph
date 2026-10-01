package io.extendedotel.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Official OTLP Protobuf decoding followed by deterministic semantic contribution extraction. */
public final class MetricParser extends RichFlatMapFunction<byte[], GraphModel.Contribution> {
  public static final String REQUEST_TOTAL = "traces_service_graph_request_total";
  public static final String DISCOVERY = "semconv.graph.discovery.calls";
  private static final Set<String> SUPPORTED = Set.of(REQUEST_TOTAL, DISCOVERY);
  private static final Logger LOG = LoggerFactory.getLogger(MetricParser.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  public record Rejection(String reason, String detail) {}

  public record Parsed(List<GraphModel.Contribution> mutations, List<Rejection> rejections) {}

  private transient Map<String, Counter> rejectionCounters;
  private transient Counter rejectedInputs;
  private transient RejectionLog rejectionLog;

  @Override
  public void open(OpenContext context) {
    rejectionLog = new RejectionLog(LOG);
    rejectionCounters = new LinkedHashMap<>();
    rejectedInputs = getRuntimeContext().getMetricGroup().counter("rejected_inputs");
  }

  @Override
  public void flatMap(byte[] payload, Collector<GraphModel.Contribution> output) {
    Parsed parsed = parse(payload);
    parsed.mutations().forEach(output::collect);
    for (Rejection rejection : parsed.rejections()) {
      rejectedInputs.inc();
      rejectionCounters
          .computeIfAbsent(
              rejection.reason(),
              reason ->
                  getRuntimeContext()
                      .getMetricGroup()
                      .addGroup("ingest_rejections")
                      .counter(reason))
          .inc();
      rejectionLog.report(rejection);
    }
  }

  public static Parsed parse(byte[] payload) {
    var output = new ArrayList<GraphModel.Contribution>();
    var rejected = new ArrayList<Rejection>();
    ExportMetricsServiceRequest request;
    try {
      request = ExportMetricsServiceRequest.parseFrom(payload);
    } catch (InvalidProtocolBufferException error) {
      return new Parsed(
          List.of(),
          List.of(new Rejection("invalid_otlp_protobuf", error.getClass().getSimpleName())));
    }
    for (var resource : request.getResourceMetricsList())
      for (var scope : resource.getScopeMetricsList())
        for (Metric metric : scope.getMetricsList()) {
          if (!SUPPORTED.contains(metric.getName())) continue;
          if (!metric.hasSum()) {
            rejected.add(
                new Rejection(
                    "invalid_servicegraph_metric_type", "supported metric must be an OTLP Sum"));
            continue;
          }
          if (metric.getSum().getAggregationTemporality()
              != AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA) {
            rejected.add(
                new Rejection(
                    "invalid_servicegraph_temporality",
                    "supported metric must use delta temporality"));
            continue;
          }
          for (NumberDataPoint point : metric.getSum().getDataPointsList()) {
            boolean discovery = metric.getName().equals(DISCOVERY);
            String reason = "invalid_servicegraph_datapoint";
            try {
              Number value =
                  switch (point.getValueCase()) {
                    case AS_INT -> Long.valueOf(point.getAsInt());
                    case AS_DOUBLE -> Double.valueOf(point.getAsDouble());
                    default -> throw new IllegalArgumentException("datapoint has no numeric value");
                  };
              BigInteger observed = new BigInteger(Long.toUnsignedString(point.getTimeUnixNano()));
              if (!Double.isFinite(value.doubleValue())
                  || value.doubleValue() <= 0
                  || observed.signum() <= 0)
                throw new IllegalArgumentException(
                    "datapoint requires positive finite value and positive timestamp");
              reason = discovery ? "invalid_discovery_datapoint" : reason;
              var attributes = scalarAttributes(point.getAttributesList());
              if (discovery) {
                output.addAll(discovery(attributes, observed));
              } else {
                Parsed parsed = servicegraph(attributes, observed);
                output.addAll(parsed.mutations());
                rejected.addAll(parsed.rejections());
              }
            } catch (IllegalArgumentException error) {
              rejected.add(new Rejection(reason, error.getMessage()));
            }
          }
        }
    return new Parsed(List.copyOf(output), List.copyOf(rejected));
  }

  public static Map<String, Object> scalarAttributes(List<KeyValue> entries) {
    var result = new LinkedHashMap<String, Object>();
    for (var entry : entries) {
      AnyValue value = entry.getValue();
      Object decoded =
          switch (value.getValueCase()) {
            case STRING_VALUE -> value.getStringValue();
            case BOOL_VALUE -> value.getBoolValue();
            case INT_VALUE -> value.getIntValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            default -> null;
          };
      if (decoded != null) result.put(entry.getKey(), decoded);
    }
    return new LinkedHashMap<>(new TreeMap<>(result));
  }

  private static List<GraphModel.Contribution> discovery(
      Map<String, Object> attributes, BigInteger observed) {
    var output = new ArrayList<GraphModel.Contribution>();
    for (var element :
        materializeObservation(
            extractEntities(attributes, SemanticRegistry.EvidenceSource.SPAN_METRICS),
            SemanticRegistry.EvidenceSource.SPAN_METRICS)) {
      String contributor =
          "spanmetrics:"
              + hash(
                  ordered(
                      "source", DISCOVERY,
                      "element_id", element.id(),
                      "attributes", element.attributes()));
      output.add(new GraphModel.Contribution(contributor, observed, element));
    }
    return output;
  }

  private static Parsed servicegraph(Map<String, Object> attributes, BigInteger observed) {
    String client = requiredString(attributes, "client"),
        server = requiredString(attributes, "server");
    String connectionType = connectionType(attributes.get("connection_type"));
    var dimensions = new LinkedHashMap<>(attributes);
    Set.of("client", "server", "connection_type").forEach(dimensions::remove);
    String contributor =
        hash(
            ordered(
                "client", client,
                "server", server,
                "connection_type", connectionType,
                "dimensions", dimensions));
    var clients = observation(attributes, "client", client);
    var servers = observation(attributes, "server", server);
    var elements = new TreeMap<String, GraphModel.Element>();
    for (var entities : List.of(clients, servers)) {
      for (var element :
          materializeObservation(entities, SemanticRegistry.EvidenceSource.SERVICE_GRAPH))
        add(elements, element);
    }
    var rejections = new ArrayList<Rejection>();
    if (SemanticRegistry.INSTANCE.supportsConnectionType(
        SemanticRegistry.EvidenceSource.SERVICE_GRAPH, connectionType)) {
      for (var edge :
          SemanticRegistry.INSTANCE.edges(
              clients,
              servers,
              SemanticRegistry.EvidenceSource.SERVICE_GRAPH,
              SemanticRegistry.RelationshipScope.INTERACTION,
              connectionType)) add(elements, edge);
    } else {
      rejections.add(new Rejection("unsupported_servicegraph_connection_type", connectionType));
    }
    var mutations =
        elements.values().stream()
            .<GraphModel.Contribution>map(
                item -> new GraphModel.Contribution(contributor, observed, item))
            .toList();
    return new Parsed(mutations, List.copyOf(rejections));
  }

  private static List<GraphModel.Element> observation(
      Map<String, Object> attributes, String side, String name) {
    var selected = new LinkedHashMap<String, Object>();
    String prefix = side + "_";
    attributes.forEach(
        (key, value) -> {
          if (key.startsWith(prefix)) selected.put(key.substring(prefix.length()), value);
        });
    selected.putIfAbsent("service.name", name);
    return extractEntities(selected, SemanticRegistry.EvidenceSource.SERVICE_GRAPH);
  }

  private static List<GraphModel.Element> extractEntities(
      Map<String, Object> attributes, SemanticRegistry.EvidenceSource evidenceSource) {
    return SemanticRegistry.INSTANCE.extractValid(attributes).stream()
        .filter(entity -> SemanticRegistry.INSTANCE.supportsEntity(evidenceSource, entity.type()))
        .sorted(java.util.Comparator.comparing(GraphModel.Element::id))
        .toList();
  }

  private static List<GraphModel.Element> materializeObservation(
      List<GraphModel.Element> entities, SemanticRegistry.EvidenceSource evidenceSource) {
    var elements = new TreeMap<String, GraphModel.Element>();
    entities.forEach(entity -> add(elements, entity));
    SemanticRegistry.INSTANCE
        .edges(entities, entities, evidenceSource, SemanticRegistry.RelationshipScope.OBSERVATION)
        .forEach(edge -> add(elements, edge));
    return List.copyOf(elements.values());
  }

  private static String connectionType(Object value) {
    if (value == null) return "unset";
    if (!(value instanceof String text))
      throw new IllegalArgumentException("connection_type must be a string");
    return text.isEmpty() || text.equals("unset") ? "unset" : text;
  }

  private static void add(Map<String, GraphModel.Element> elements, GraphModel.Element element) {
    var old = elements.get(element.id());
    if (old == null) {
      elements.put(element.id(), element);
      return;
    }
    if (!old.kind().equals(element.kind())
        || !old.type().equals(element.type())
        || !java.util.Objects.equals(old.sourceId(), element.sourceId())
        || !java.util.Objects.equals(old.targetId(), element.targetId()))
      throw new IllegalArgumentException("conflicting graph identity");
    var merged = new LinkedHashMap<>(old.attributes());
    element
        .attributes()
        .forEach(
            (key, value) ->
                merged.merge(
                    key,
                    value,
                    (left, right) -> json(left).compareTo(json(right)) <= 0 ? left : right));
    var updated = element.withAttributes(merged);
    elements.put(element.id(), updated);
  }

  public static String requiredString(Map<String, Object> attributes, String key) {
    if (!(attributes.get(key) instanceof String text) || text.isEmpty())
      throw new IllegalArgumentException("missing nonempty attribute " + key);
    return text;
  }

  public static String boundedDetail(String detail) {
    return detail == null ? "" : detail.substring(0, Math.min(detail.length(), 512));
  }

  private static Map<String, Object> ordered(Object... fields) {
    var value = new LinkedHashMap<String, Object>();
    for (int index = 0; index < fields.length; index += 2)
      value.put((String) fields[index], fields[index + 1]);
    return value;
  }

  private static String hash(Object value) {
    try {
      return Sha256.digest(JSON.writeValueAsBytes(value));
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("metric identity cannot be encoded as JSON", exception);
    }
  }

  private static String json(Object value) {
    try {
      return JSON.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("metric attribute cannot be encoded as JSON", exception);
    }
  }
}
