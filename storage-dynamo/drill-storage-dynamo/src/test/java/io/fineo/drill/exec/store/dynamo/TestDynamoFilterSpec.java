package io.fineo.drill.exec.store.dynamo;

import io.fineo.drill.exec.store.dynamo.spec.DynamoFilterSpec;
import org.junit.Test;

import static io.fineo.drill.exec.store.dynamo.spec.DynamoFilterSpec.create;
import static org.junit.Assert.assertEquals;

public class TestDynamoFilterSpec {

  @Test
  public void testSimpleEquality() throws Exception {
    assertExpressionEquals("a = b", create("equal", "a", "b"));
    assertExpressionEquals("a <> b", create("not_equal", "a", "b"));
    assertExpressionEquals("a < b", create("less_than", "a", "b"));
    assertExpressionEquals("a <= b", create("less_than_or_equal_to", "a", "b"));
    assertExpressionEquals("a > b", create("greater_than", "a", "b"));
    assertExpressionEquals("a >= b", create("greater_than_or_equal_to", "a", "b"));
  }

  @Test
  public void testExists() throws Exception {
    assertExpressionEquals("attribute_exists(a)", create("isNotNull", "a"));
    assertExpressionEquals("attribute_not_exists(a)", create("isNull", "a"));
  }

  @Test
  public void testBetween() throws Exception {
    assertExpressionEquals("a BETWEEN 1 AND 2", create("between", "a", 1, 2));
  }

  private static void assertExpressionEquals(String expr, DynamoFilterSpec spec) {
    assertEquals(expr, spec.getTree().getRoot().toString());
  }
}
