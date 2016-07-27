package io.fineo.read.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import io.fineo.drill.exec.store.dynamo.DynamoPlanValidationUtils;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoQueryFilterSpec;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.read.drill.PlanValidator;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.StoreClerk;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static org.apache.calcite.util.ImmutableNullableList.of;
import static org.junit.Assert.assertNotEquals;

/**
 * Do Fineo-style reads over a dynamo table
 */
public class TestFineoOverDynamo extends BaseFineoTest {

  @Test
  public void testReadSingleRow() throws Exception {
    TestState state = register();
    long ts = FineoTestUtil.get1980();

    StoreClerk clerk = new StoreClerk(state.getStore(), org);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metrictype);

    Item wrote = new Item();
    wrote.with(Schema.PARTITION_KEY_NAME, org + metric.getMetricId());
    wrote.with(Schema.SORT_KEY_NAME, ts);
    wrote.with("field1", true);
    Table table = state.write(wrote);
    bootstrap(table);

    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaEncoder.ORG_ID_KEY, org);
    expected.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(TIMESTAMP_KEY, ts);
    expected.put("field1", true);
    verifySelectStar(FineoTestUtil.withNext(expected));
  }

  @Test
  public void testFilterTableOnTimeRange() throws Exception {
    TestState state = register();
    long ts = FineoTestUtil.get1980();

    StoreClerk clerk = new StoreClerk(state.getStore(), org);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metrictype);

    String key = org + metric.getMetricId();
    Item wrote = new Item();
    wrote.with(Schema.PARTITION_KEY_NAME, key);
    wrote.with(Schema.SORT_KEY_NAME, ts);
    wrote.with("field1", true);
    Table table = state.write(wrote);

    wrote.with(Schema.SORT_KEY_NAME, ts + Duration.ofDays(30).toMillis());
    Table table2 = state.write(wrote);
    assertNotEquals("Write are to the same table, but checking reading across multiple tables! ",
      table.getTableName(), table2.getTableName());
    bootstrap(table, table2);

    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaEncoder.ORG_ID_KEY, org);
    expected.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(TIMESTAMP_KEY, ts);
    expected.put("field1", true);
    String query =
      verifySelectStar(of(FineoTestUtil.bt(TIMESTAMP_KEY) + " <= " + (ts + 100)), FineoTestUtil
        .withNext(expected));
    new PlanValidator(query)
      .validateDynamoQuery(0)
      .withTable(table)
      .withGetOrQueries(
        new DynamoQueryFilterSpec(DynamoPlanValidationUtils.equals(Schema.PARTITION_KEY_NAME,
          key), null)).done()
      .validate(drill.getConnection());
  }

  private void bootstrap(Table... tables) throws IOException {
    BootstrapFineo bootstrap = new BootstrapFineo();
    BootstrapFineo.DrillConfigBuilder builder =
      basicBootstrap(bootstrap.builder())
        .withDynamoKeyMapper();
    for (Table table : tables) {
      builder.withDynamoTable(table);
    }
    bootstrap.strap(builder);
  }
}
