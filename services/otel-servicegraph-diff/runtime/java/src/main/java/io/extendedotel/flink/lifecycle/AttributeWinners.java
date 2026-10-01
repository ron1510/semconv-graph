package io.extendedotel.flink.lifecycle;

import io.extendedotel.flink.model.Element;
import io.extendedotel.flink.model.Node;
import io.extendedotel.flink.model.Snapshot;
import io.extendedotel.flink.model.State;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Materialized aggregate and attribute ownership. Contributor snapshots remain in keyed MapState.
 */
public record AttributeWinners(Element element, Map<String, String> owners, int contributorCount) {
  public AttributeWinners {
    owners = Map.copyOf(owners);
    if (contributorCount <= 0 || !owners.keySet().equals(element.attributes().keySet())) {
      throw new IllegalArgumentException("invalid aggregate attribute index");
    }
  }

  @FunctionalInterface
  interface Snapshots {
    Snapshot get(String contributorId) throws Exception;
  }

  static AttributeWinners rebuild(State state) {
    Map<String, String> owners = new LinkedHashMap<>();
    for (var entry : state.contributors().entrySet()) {
      for (String name : entry.getValue().element().attributes().keySet()) {
        String owner = owners.get(name);
        if (owner == null
            || GraphLifecycle.wins(
                entry.getKey(), entry.getValue(), owner, state.contributors().get(owner))) {
          owners.put(name, entry.getKey());
        }
      }
    }
    return new AttributeWinners(
        GraphLifecycle.merge(state.elementId(), state.contributors()),
        owners,
        state.contributors().size());
  }

  boolean losesAttribute(String contributorId, Element replacement) {
    return owners.entrySet().stream()
        .anyMatch(
            entry ->
                entry.getValue().equals(contributorId)
                    && !replacement.attributes().containsKey(entry.getKey()));
  }

  AttributeWinners observe(
      String contributorId, Snapshot snapshot, boolean existing, Snapshots snapshots)
      throws Exception {
    Element incoming = snapshot.element();
    if (!element.id().equals(incoming.id())
        || !element.kind().equals(incoming.kind())
        || !element.type().equals(incoming.type())
        || !Objects.equals(element.sourceId(), incoming.sourceId())
        || !Objects.equals(element.targetId(), incoming.targetId())) {
      throw new IllegalArgumentException(
          "contribution conflicts with graph element identity " + element.id());
    }
    Map<String, String> nextOwners = new LinkedHashMap<>(owners);
    Map<String, Object> attributes = new LinkedHashMap<>(element.attributes());
    for (var attribute : incoming.attributes().entrySet()) {
      String owner = owners.get(attribute.getKey());
      if (owner == null
          || owner.equals(contributorId)
          || GraphLifecycle.wins(contributorId, snapshot, owner, snapshots.get(owner))) {
        nextOwners.put(attribute.getKey(), contributorId);
        attributes.put(attribute.getKey(), attribute.getValue());
      }
    }
    Element merged =
        element instanceof Node
            ? Element.node(element.id(), element.type(), attributes)
            : Element.edge(
                element.id(), element.type(), element.sourceId(), element.targetId(), attributes);
    return new AttributeWinners(merged, nextOwners, contributorCount + (existing ? 0 : 1));
  }
}
