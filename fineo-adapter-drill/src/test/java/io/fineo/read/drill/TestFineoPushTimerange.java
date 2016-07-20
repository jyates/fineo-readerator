package io.fineo.read.drill;

import io.fineo.drill.rule.DrillClusterRule;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.test.rule.TestOutput;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensure that we push the timerange down into the scan when applicable
 */
public class TestFineoPushTimerange extends BaseDynamoTableTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFineoPushTimerange.class);

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @Rule
  public TestOutput folder = new TestOutput(false);

  @Test
  public void testPushTimerangeIntoFileQuery() throws Exception {

  }
}
