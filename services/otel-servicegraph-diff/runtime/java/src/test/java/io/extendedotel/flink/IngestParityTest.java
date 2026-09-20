package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IngestParityTest {
  private static final ObjectMapper JSON =
      JsonMapper.builder().enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS).build();

  private static JsonNode fixture() throws Exception {
    try (var input = IngestParityTest.class.getResourceAsStream("/ingest-golden.json")) {
      assertNotNull(input);
      return JSON.readTree(input);
    }
  }

  private static Map<String, Object> map(JsonNode node) {
    return CanonicalJson.parseObject(node.toString());
  }

  private static void same(JsonNode expected, Object actual, String name) throws Exception {
    assertEquals(
        CanonicalJson.stringify(JSON.readValue(expected.toString(), Object.class)),
        CanonicalJson.stringify(actual),
        name);
  }

  @Test
  void allGeneratedSemanticEntitiesPreserveSdkIdentitiesAttributesAndTemplates() throws Exception {
    for (var item : fixture().get("semantics"))
      same(
          item.get("elements"),
          SemanticRegistry.INSTANCE.extract(map(item.get("attributes"))).stream()
              .map(GraphModel.Element::toMap)
              .toList(),
          item.get("name").asText());
  }

  @Test
  void allModeledFieldsRejectWrongTypesIncludingTemplatesArraysEnumsAndNonfiniteNumbers()
      throws Exception {
    for (var item : fixture().get("semantic_rejections"))
      assertThrows(
          IllegalArgumentException.class,
          () -> SemanticRegistry.INSTANCE.extract(map(item.get("attributes"))),
          item.get("name").asText());
  }
}
