package io.fineo.read.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import io.fineo.drill.ClusterTest;
import io.fineo.drill.exec.store.dynamo.DynamoPlanValidationUtils;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoQueryFilterSpec;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.read.drill.PlanValidator;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.fineo.drill.exec.store.dynamo.DynamoPlanValidationUtils.lte;
import static io.fineo.read.drill.FineoTestUtil.get1980;
import static io.fineo.read.drill.FineoTestUtil.p;
import static io.fineo.read.drill.FineoTestUtil.withNext;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static org.apache.calcite.util.ImmutableNullableList.of;
import static org.junit.Assert.assertNotEquals;

/**
 * Do Fineo-style reads over a dynamo table
 */
@Category(ClusterTest.class)
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
    long tsLessThan = (ts + 100);
    String query =
      verifySelectStar(of(FineoTestUtil.bt(TIMESTAMP_KEY) + " <= " + tsLessThan), FineoTestUtil
        .withNext(expected));
    // take into account the push down timerange filter, which includes the TS in the output, but
    // otherwise wouldn't be there.
    DynamoFilterSpec keyFilter = DynamoPlanValidationUtils.equals(Schema.PARTITION_KEY_NAME,
      key).and(lte(Schema.SORT_KEY_NAME, tsLessThan));
    new PlanValidator(query)
      .validateDynamoQuery()
      .withTable(table)
      .withGetOrQueries(
        new DynamoQueryFilterSpec(keyFilter, null)).done()
      .validate(drill.getConnection());
  }

  /**
   * Basic test that we actually are copy the rows up correctly. This could be done for dynamo,
   * json, or parquet, but dynamo was the easiest at the time.
   */
  @Test
  public void testReadMultipleRows() throws Exception {
    TestState state = register(p(fieldname, StoreManager.Type.INT));
    long ts = get1980();
    Item dynamo = prepareItem(state);
    dynamo.with(Schema.SORT_KEY_NAME, ts);
    dynamo.with(fieldname, 1);
    Table table = state.write(dynamo);

    // definitely a different row
    table.putItem(dynamo.with(fieldname, 25).with(Schema.SORT_KEY_NAME, ts + 1));

    bootstrap(table);
    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaEncoder.ORG_ID_KEY, org);
    expected.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(TIMESTAMP_KEY, ts);
    expected.put(fieldname, 1);

    Map<String, Object> expected2 = new HashMap<>();
    expected2.put(AvroSchemaEncoder.ORG_ID_KEY, org);
    expected2.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metrictype);
    expected2.put(TIMESTAMP_KEY, ts+1);
    expected2.put(fieldname, 25);
    verifySelectStar(withNext(expected, expected2));
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
