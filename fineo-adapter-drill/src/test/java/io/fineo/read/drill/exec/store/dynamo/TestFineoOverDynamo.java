package io.fineo.read.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ImmutableList;
import io.fineo.drill.ClusterTest;
import io.fineo.drill.exec.store.dynamo.DynamoPlanValidationUtils;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoQueryFilterSpec;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.read.drill.PlanValidator;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static io.fineo.drill.exec.store.dynamo.DynamoPlanValidationUtils.lte;
import static io.fineo.read.drill.FineoTestUtil.get1980;
import static io.fineo.read.drill.FineoTestUtil.p;
import static io.fineo.read.drill.FineoTestUtil.withNext;
import static java.lang.String.format;
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

    Map<String, Object> wrote = prepareItem();
    wrote.put(Schema.SORT_KEY_NAME, ts);
    wrote.put("field1", true);
    Table table = state.writeToDynamo(wrote);
    bootstrap(table);
    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaProperties.ORG_ID_KEY, org);
    expected.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    expected.put("field1", true);
    String query = verifySelectStar(withNext(expected));
    DynamoFilterSpec keyFilter = getFilterSpec(state.getStore(), org, metrictype);
    new PlanValidator(query)
      // dynamo
      .validateDynamoQuery()
      .withTable(table)
      .withGetOrQueries(
        new DynamoQueryFilterSpec(keyFilter, null))
      // rest of the query planning above dynamo
      .withNextStep("project")
      .withNextStep("dynamo-expander")
      .withNextStep("project")
      .withNextStep("fineo-recomb")
      .withNextStep("external-sort")
      .withNextStep("selection-vector-remover")
      .withNextStep("project")
      .withNextStep("screen")
      .done()
      .validate(drill.getConnection());
  }

  public static DynamoFilterSpec getFilterSpec(SchemaStore store, String org, String metricName)
    throws SchemaNotFoundException {
    String metricId = new StoreClerk(store, org).getMetricForUserNameOrAlias
      (metricName).getMetricId();
    return
      DynamoPlanValidationUtils.equals(Schema.PARTITION_KEY_NAME, org + metricId);
  }

  @Test
  public void testReadOverlappingTimestampRow() throws Exception {
    String field = "field1";
    TestState state = register(new ImmutablePair<>(field, StoreManager.Type.STRING));
    long ts = FineoTestUtil.get1980();

    Map<String, Object> wrote = prepareItem();
    wrote.put(Schema.SORT_KEY_NAME, ts);
    wrote.put(field, "v1");
    Table table = state.writeToDynamo(wrote);
    bootstrap(table);

    Map<String, Object> wrote2 = newHashMap(wrote);
    wrote2.put(field, "v2");
    state.update(wrote2);

    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaProperties.ORG_ID_KEY, org);
    expected.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    expected.put(field, "v1");
    Map<String, Object> expected2 = newHashMap(expected);
    expected2.put(field, "v2");
    QueryRunnable runnable = new QueryRunnable(ImmutableList.of(), withNext(expected,
      expected2));
    runnable.sortBy(field);
    runAndVerify(runnable);
  }

  @Test
  public void testFilterTableOnTimeRange() throws Exception {
    TestState state = register();
    long ts = FineoTestUtil.get1980();

    StoreClerk clerk = new StoreClerk(state.getStore(), org);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metrictype);

    String key = org + metric.getMetricId();
    Map<String, Object> wrote = prepareItem();
    wrote.put(Schema.SORT_KEY_NAME, ts);
    wrote.put("field1", true);
    Table table = state.writeToDynamo(wrote);

    wrote.put(Schema.SORT_KEY_NAME, ts + Duration.ofDays(30).toMillis());
    Table table2 = state.writeToDynamo(wrote);
    assertNotEquals("Write are to the same table, but checking reading across multiple tables! ",
      table.getTableName(), table2.getTableName());
    bootstrap(table, table2);

    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaProperties.ORG_ID_KEY, org);
    expected.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    expected.put("field1", true);
    long tsLessThan = (ts + 100);
    String query =
      verifySelectStar(
        of(FineoTestUtil.bt(AvroSchemaProperties.TIMESTAMP_KEY) + " <= " + tsLessThan),
        withNext(expected));
    // take into account the push down timerange filter, which includes the TS in the output, but
    // otherwise wouldn't be there.
    DynamoFilterSpec keyFilter = DynamoPlanValidationUtils.equals(Schema.PARTITION_KEY_NAME,
      key).and(lte(Schema.SORT_KEY_NAME, tsLessThan));
    new PlanValidator(query)
      .validateDynamoQuery()
      .withTable(table)
      .withGetOrQueries(
        new DynamoQueryFilterSpec(keyFilter, null))
      .withNextStep("project")
      .withNextStep("dynamo-expander")
      .withNextStep("project")
      .withNextStep("fineo-recomb")
      .withNextStep("filter")
      .done()
      .validate(drill.getConnection());
  }

  /**
   * Simple check for a single table that we always include it when filtering by timestamp.
   * Covers regression testing for dynamo range filtering
   *
   * @throws Exception on failure
   */
  @Test
  public void testFilterRangeOneTable() throws Exception {
    TestState state = register();
    long ts = FineoTestUtil.get1980();

    Map<String, Object> wrote = prepareItem();
    wrote.put(Schema.SORT_KEY_NAME, ts);
    wrote.put("field1", true);
    Table table = state.writeToDynamo(wrote);
    bootstrap(table);

    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaProperties.ORG_ID_KEY, org);
    expected.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    expected.put("field1", true);
    verifySelectStarWhereTimestamp("<", ts + 1, expected);
    verifySelectStarWhereTimestamp("<=", ts + 1, expected);
    verifySelectStarWhereTimestamp("=", ts, expected);

    verifySelectStarWhereTimestamp("<>", ts + 1, expected);
    verifySelectStarWhereTimestamp("<>", ts - 1, expected);
    verifySelectStarWhereTimestamp("<>", ts);

    verifySelectStarWhereTimestamp(">=", ts - 1, expected);
    verifySelectStarWhereTimestamp(">", ts - 1, expected);
  }

  private void verifySelectStarWhereTimestamp(String op, Object value, Map<String, Object>...
    expected) throws Exception {
    verifySelectStar(
      of(FineoTestUtil.bt(AvroSchemaProperties.TIMESTAMP_KEY) + format(" %s %s", op, value)),
      withNext(expected));
  }

  /**
   * Basic test that we actually are copy the rows up correctly. This could be done for dynamo,
   * json, or parquet, but dynamo was the easiest at the time.
   */
  @Test
  public void testReadMultipleRows() throws Exception {
    TestState state = register(p(fieldname, StoreManager.Type.INT));
    long ts = get1980();
    Map<String, Object> dynamo = prepareItem();
    dynamo.put(Schema.SORT_KEY_NAME, ts);
    dynamo.put(fieldname, 1);
    Table table = state.writeToDynamo(dynamo);

    // definitely a different row
    dynamo.put(fieldname, 25);
    dynamo.put(Schema.SORT_KEY_NAME, ts + 1);
    state.writeToDynamo(dynamo);

    bootstrap(table);
    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaProperties.ORG_ID_KEY, org);
    expected.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    expected.put(fieldname, 1);

    verifySelectStar(
      withNext(expected, copyOverride(expected, p(AvroSchemaProperties.TIMESTAMP_KEY, ts + 1), p
        (fieldname, 25))));
  }

  @Test
  public void testReadMultipleRowsWithMultipleEventsPerTimestamp() throws Exception {
    TestState state = register(p(fieldname, StoreManager.Type.INT));
    long ts = get1980();
    Map<String, Object> dynamo = prepareItem();
    dynamo.put(Schema.SORT_KEY_NAME, ts);
    dynamo.put(fieldname, 1);
    Table table = state.writeToDynamo(dynamo);
    dynamo.put(fieldname, 2);
    state.update(dynamo);

    // definitely a different row
    dynamo.put(fieldname, 25);
    dynamo.put(Schema.SORT_KEY_NAME, ts + 1);
    state.writeToDynamo(dynamo);
    dynamo.put(fieldname, 26);
    state.update(dynamo);

    bootstrap(table);
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaProperties.ORG_ID_KEY, org);
    expected.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    expected.put(fieldname, 1);
    rows.add(expected);
    rows.add(copyOverride(expected, p(fieldname, 2)));
    Map<String, Object> row2 = copyOverride(expected, p(AvroSchemaProperties.TIMESTAMP_KEY, ts +
                                                                                            1),
      p(fieldname, 25));
    rows.add(row2);
    rows.add(copyOverride(row2, p(fieldname, 26)));
    QueryRunnable runnable = new QueryRunnable(ImmutableList.of(), withNext(rows));
    runnable.sortBy(fieldname);
    runAndVerify(runnable);
  }

  @Test
  public void testReadSingleFieldOneRow() throws Exception {
    TestState state = register();
    long ts = FineoTestUtil.get1980();

    Map<String, Object> wrote = prepareItem();
    wrote.put(Schema.SORT_KEY_NAME, ts);
    wrote.put(fieldname, true);
    Table table = state.writeToDynamo(wrote);
    bootstrap(table);

    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaProperties.ORG_ID_KEY, org);
    expected.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(fieldname, true);
    runAndVerify(new QueryRunnable(withNext(expected)).select(fieldname));
  }

  @Test
  public void testReadSingleFieldMultipleElementsPerRow() throws Exception {
    TestState state = register(p(fieldname, StoreManager.Type.INTEGER));
    long ts = FineoTestUtil.get1980();

    Map<String, Object> wrote = prepareItem();
    wrote.put(Schema.SORT_KEY_NAME, ts);
    wrote.put(fieldname, 1);
    Table table = state.writeToDynamo(wrote);
    bootstrap(table);

    wrote.put(fieldname, 25);
    state.update(wrote);

    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaProperties.ORG_ID_KEY, org);
    expected.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(fieldname, 1);
    runAndVerify(new QueryRunnable(withNext(expected, copyOverride(expected, p(fieldname, 25))))
      .select(fieldname).sortBy(fieldname));
  }

  private Map<String, Object> copyOverride(Map<String, Object> map, Pair<String, Object>...
    overrides) {
    Map<String, Object> copy = newHashMap(map);
    for (Pair<String, Object> override : overrides) {
      copy.put(override.getKey(), override.getValue());
    }
    return copy;
  }

  private void bootstrap(Table... tables) throws IOException {
    BootstrapFineo bootstrap = newBootstrap();
    BootstrapFineo.DrillConfigBuilder builder =
      basicBootstrap(bootstrap.builder())
        .withDynamoKeyMapper();
    for (Table table : tables) {
      builder.withDynamoTable(table);
    }
    bootstrap.strap(builder);
  }
}
