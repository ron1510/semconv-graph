package io.extendedotel.flink.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class ModelTest {
  @Test
  void elementsRecursivelyOrderAndFreezeAttributesAndRoundTripMaps() {
    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("z", 1L);
    nested.put("a", List.of(Map.of("second", true, "first", "value")));
    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("z", "last");
    attributes.put("nested", nested);
    attributes.put("a", "first");

    Element node = Element.node("service:checkout", "service", attributes);

    assertEquals(List.of("a", "nested", "z"), new ArrayList<>(node.attributes().keySet()));
    assertEquals(
        List.of("a", "z"),
        new ArrayList<>(
            ((Map<?, ?>) node.attributes().get("nested"))
                .keySet().stream().map(Object::toString).toList()));
    assertEquals(node, Element.fromMap(node.toMap()));
    assertThrows(UnsupportedOperationException.class, () -> node.attributes().put("new", "value"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> ((Map<String, Object>) node.attributes().get("nested")).put("new", "value"));
  }

  @Test
  void edgeMapRoundTripPreservesIdentityAndEndpoints() {
    Element edge =
        Element.edge(
            "edge:stable", "calls", "service:client", "service:server", Map.of("zone", "a"));

    assertEquals(edge, Element.fromMap(edge.toMap()));
  }
}
