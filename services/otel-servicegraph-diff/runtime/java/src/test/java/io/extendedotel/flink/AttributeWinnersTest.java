package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.*;

import io.extendedotel.flink.GraphModel.*;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

final class AttributeWinnersTest {
  @Test
  void incrementalRefreshMatchesFullMergeWithTiesAndPartialAttributes() throws Exception {
    Random random = new Random(72);
    State state = null;
    AttributeWinners index = null;
    for (int step = 0; step < 2000; step++) {
      String id = "contributor-" + random.nextInt(128);
      Map<String, Object> attributes = new LinkedHashMap<>();
      for (int attribute = 0; attribute < 8; attribute++) {
        if (random.nextBoolean()) attributes.put("field-" + attribute, random.nextInt(5));
      }
      Element edge = Element.edge("edge:shared", "calls", "service:a", "service:b", attributes);
      Contribution observation = new Contribution(id, BigInteger.valueOf(1 + step / 4), edge);
      State previous = state;
      Result result = GraphLifecycle.apply(state, observation, 86400, 0, step, step);
      state = result.state();
      if (index == null || index.losesAttribute(id, edge)) {
        index = AttributeWinners.rebuild(state);
      } else {
        index =
            index.observe(
                id,
                state.contributors().get(id),
                previous.contributors().containsKey(id),
                previous.contributors()::get);
      }
      assertEquals(GraphLifecycle.merge(state.elementId(), state.contributors()), index.element());
      assertEquals(state.contributors().size(), index.contributorCount());
      assertEquals(state.lastPayloadHash(), GraphLifecycle.payloadHash(index.element()));
    }
  }
}
