package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class SemanticRegistryTest {
  @Test
  void edgeExpansionUsesOnlyRegisteredSourceTypeRelationshipAndTargetType() {
    var clientService = node("service:client", "service");
    var clientPod = node("k8s.pod:client", "k8s.pod");
    var serverService = node("service:server", "service");
    var repository = node("vcs.repository:repo", "vcs.repository");

    var calls =
        SemanticRegistry.INSTANCE.edges(
            List.of(clientService, clientPod),
            List.of(serverService, repository),
            SemanticRegistry.EvidenceSource.SERVICE_GRAPH,
            SemanticRegistry.RelationshipScope.INTERACTION,
            "unset");

    assertEquals(1, calls.size());
    assertEquals(
        GraphModel.Element.edge(
            SemanticRegistry.edgeId(clientService.id(), "calls", serverService.id()),
            "calls",
            clientService.id(),
            serverService.id(),
            Map.of()),
        calls.get(0));
    assertTrue(
        SemanticRegistry.INSTANCE
            .edges(
                List.of(clientPod),
                List.of(serverService),
                SemanticRegistry.EvidenceSource.SERVICE_GRAPH,
                SemanticRegistry.RelationshipScope.INTERACTION,
                "unset")
            .isEmpty());
    assertTrue(
        SemanticRegistry.INSTANCE
            .edges(
                List.of(clientService),
                List.of(repository),
                SemanticRegistry.EvidenceSource.SERVICE_GRAPH,
                SemanticRegistry.RelationshipScope.INTERACTION,
                "unknown")
            .isEmpty());
  }

  @Test
  void unconstrainedExpansionCreatesEveryAndOnlyMatchingRegisteredEdge() {
    var service = node("service:checkout", "service");
    var pod = node("k8s.pod:checkout", "k8s.pod");
    var repository = node("vcs.repository:checkout", "vcs.repository");

    var triples =
        SemanticRegistry.INSTANCE
            .edges(
                List.of(service, pod),
                List.of(service, repository),
                SemanticRegistry.EvidenceSource.SERVICE_GRAPH,
                SemanticRegistry.RelationshipScope.OBSERVATION)
            .stream()
            .map(edge -> edge.sourceId() + " " + edge.type() + " " + edge.targetId())
            .toList();

    assertEquals(
        List.of(
            "k8s.pod:checkout runs service:checkout",
            "service:checkout built_from vcs.repository:checkout"),
        triples);
  }

  @Test
  void edgeExpansionSuppressesSelfLoops() {
    var service = node("service:checkout", "service");

    var calls =
        SemanticRegistry.INSTANCE.edges(
            List.of(service),
            List.of(service),
            SemanticRegistry.EvidenceSource.SERVICE_GRAPH,
            SemanticRegistry.RelationshipScope.INTERACTION,
            "unset");

    assertTrue(calls.isEmpty());
  }

  @Test
  void evidenceSourcesSelectEntitiesAndObservationRelationships() {
    var service = node("service:worker", "service");
    var transaction = node("transaction:worker:consume:SPAN_KIND_INTERNAL", "transaction");

    assertTrue(
        SemanticRegistry.INSTANCE
            .edges(
                List.of(service, transaction),
                List.of(service, transaction),
                SemanticRegistry.EvidenceSource.SERVICE_GRAPH,
                SemanticRegistry.RelationshipScope.OBSERVATION)
            .stream()
            .noneMatch(edge -> edge.type().equals("executes")));
    assertEquals(
        1,
        SemanticRegistry.INSTANCE
            .edges(
                List.of(service, transaction),
                List.of(service, transaction),
                SemanticRegistry.EvidenceSource.SPAN_METRICS,
                SemanticRegistry.RelationshipScope.OBSERVATION)
            .stream()
            .filter(edge -> edge.type().equals("executes"))
            .count());
    assertTrue(
        !SemanticRegistry.INSTANCE.supportsEntity(
            SemanticRegistry.EvidenceSource.SERVICE_GRAPH, "transaction"));
    assertTrue(
        SemanticRegistry.INSTANCE.supportsEntity(
            SemanticRegistry.EvidenceSource.SPAN_METRICS, "transaction"));
  }

  private static GraphModel.Element node(String id, String type) {
    return GraphModel.Element.node(id, type, Map.of());
  }
}
