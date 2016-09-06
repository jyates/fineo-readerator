package io.fineo.read.drill.exec.store;

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
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.fineo.drill.exec.store.dynamo.DynamoPlanValidationUtils.lte;
import static io.fineo.read.drill.FineoTestUtil.bt;
import static io.fineo.read.drill.FineoTestUtil.get1980;
import static io.fineo.read.drill.FineoTestUtil.p;
import static io.fineo.read.drill.FineoTestUtil.withNext;
import static org.apache.calcite.util.ImmutableNullableList.of;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Validate reads across all three sources with different varying time ranges. Things that need
 * covering are:
 * - ensuring that we don't have duplicate data when reading overlapping time ranges
 * - combining fields across json and parquet formats
 */
@Category(ClusterTest.class)
public class TestClientLikeReads extends BaseFineoTest {

  private static final long ONE_DAY_MILLIS = 24 * 60 * 60 * 1000;

  @Test
  public void testReadAcrossFileAndDynamo() throws Exception {
    TestState state = register(p(fieldname, StoreManager.Type.INTEGER));
    long ts = get1980();

    File tmp = folder.newFolder("drill");
    long tsFile = ts - Duration.ofDays(35).toMillis();

    Map<String, Object> parquetRow = new HashMap<>();
    parquetRow.put(fieldname, 1);
    Pair<FsSourceTable, File> parquet = writeParquet(state, tmp, org, metrictype, tsFile,
      parquetRow);

    Map<String, Object> wrote = prepareItem();
    wrote.put(Schema.SORT_KEY_NAME, ts);
    wrote.put(fieldname, 2);
    Table table = state.write(wrote);

    bootstrapper()
      // dynamo
      .withDynamoKeyMapper()
      .withDynamoTable(table)
      // fs
      .withLocalSource(parquet.getKey())
      .bootstrap();

    Map<String, Object> dynamoRow = new HashMap<>();
    dynamoRow.put(AvroSchemaProperties.ORG_ID_KEY, org);
    dynamoRow.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    dynamoRow.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    dynamoRow.put(fieldname, 2);

    verifySelectStar(FineoTestUtil.withNext(parquetRow, dynamoRow));
  }

  /**
   * This shouldn't ever happen that we have two different values in real life. This is just a
   * check to make sure that we have the right value back (from dynamo, not parquet).
   */
  @Test
  public void testReadAcrossOverlappingFileAndDynamo() throws Exception {
    TestState state = register(p(fieldname, StoreManager.Type.INT));
    long ts = get1980();
    Map<String, Object> dynamo = prepareItem();
    dynamo.put(Schema.SORT_KEY_NAME, ts);
    dynamo.put(fieldname, 1);
    Table table = state.write(dynamo);

    Map<String, Object> parquet = new HashMap<>();
    parquet.put(fieldname, 2);
    File drill = folder.newFolder("drill");
    FsSourceTable source = state.writeParquet(drill, ts, parquet);
    bootstrapper().withDynamoKeyMapper().withDynamoTable(table).withLocalSource(source).bootstrap();

    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaProperties.ORG_ID_KEY, org);
    expected.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    expected.put(fieldname, 1);
    verifySelectStar(withNext(expected));
  }


  @Test
  public void testPruneFileDirectoryAndDynamo() throws Exception {
    TestState state = register(p(fieldname, StoreManager.Type.INT));
    long ts = get1980();
    Map<String, Object> dynamo = prepareItem();
    dynamo.put(Schema.SORT_KEY_NAME, ts);
    dynamo.put(fieldname, 1);
    Table table = state.write(dynamo);

    Map<String, Object> parquet = new HashMap<>();
    parquet.put(fieldname, 2);

    File drillDir = folder.newFolder("drill");
    Pair<FsSourceTable, File> source = writeParquet(state, drillDir, org, metrictype, ts, parquet);
    BootstrapFineo.DrillConfigBuilder builder = bootstrapper().withDynamoKeyMapper()
                                                              .withDynamoTable(table)
                                                              .withLocalSource(source.getKey());
    // apply some parquet data in the future
    Map<String, Object> parquet2 = new HashMap<>();
    parquet.put(fieldname, 3);
    state.writeParquet(drillDir, ts + ONE_DAY_MILLIS * 35, parquet2);

    builder.bootstrap();

    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaProperties.ORG_ID_KEY, org);
    expected.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    expected.put(fieldname, 1);
    String query =
      verifySelectStar(of(bt(AvroSchemaProperties.TIMESTAMP_KEY) + " <= " + ts), withNext
        (expected));

    // validate that we only read the single parquet that we expected and the dynamo table
    DynamoFilterSpec keyFilter = DynamoPlanValidationUtils.equals(Schema.PARTITION_KEY_NAME,
      dynamo.get(Schema.PARTITION_KEY_NAME)).and(lte(Schema.SORT_KEY_NAME, ts));
    new PlanValidator(query)
      // dynamo
      .validateDynamoQuery()
      .withTable(table)
      .withGetOrQueries(
        new DynamoQueryFilterSpec(keyFilter, null)).done()
      // parquet
      .validateParquetScan()
      .withFiles(of(source.getValue()))
      .withFormat(ParquetFormatConfig.class)
      .withSelectionRoot(
        PlanValidator.getSelectionRoot(state.getStore(), source.getKey(), org, metrictype))
      .done()
      .validate(drill.getConnection());
  }

  @Test
  public void testStoringNonUserVisibleFieldName() throws Exception {
    TestState state = register();
    // create a new alias name for the field
    String storeFieldName = "other-field-name";
    SchemaStore store = state.getStore();
    StoreManager manager = new StoreManager(store);
    manager.updateOrg(org)
           .updateMetric(metrictype).addFieldAlias(fieldname, storeFieldName).build()
           .commit();

    // apply a file with the new field name
    File drill = folder.newFolder("drill");
    Map<String, Object> values = new HashMap<>();
    values.put(storeFieldName, false);
    FsSourceTable out = state.writeParquet(drill, 1, values);

    bootstrap(out);

    // we should read this as the client visible name
    Boolean value = (Boolean) values.remove(storeFieldName);
    values.put(fieldname, value);

    verifySelectStar(FineoTestUtil.withNext(values));
  }

  @Test
  public void testReadCanonicalNamedField() throws Exception {
    TestState state = register(p(fieldname, StoreManager.Type.INT));
    StoreClerk clerk = new StoreClerk(state.getStore(), org);
    StoreClerk.Metric metric = clerk.getMetrics().get(0);
    StoreClerk.Field field = metric.getUserVisibleFields().get(0);
    String cname = field.getCname();

    long ts = get1980();
    File drill = folder.newFolder("drill");
    long tsFile = ts - Duration.ofDays(35).toMillis();
    Map<String, Object> parquetRow = new HashMap<>();
    parquetRow.put(cname, 1);
    Pair<FsSourceTable, File> parquet = writeParquet(state, drill, org, metrictype, tsFile,
      parquetRow);

    bootstrapper()
      // fs
      .withLocalSource(parquet.getKey())
      .bootstrap();

    parquetRow.put(fieldname, parquetRow.remove(cname));
    verifySelectStar(FineoTestUtil.withNext(parquetRow));
  }

  @Test
  public void testMetricDeletionHiding() throws Exception {
    // create a single field
    Pair<String, StoreManager.Type> field = p(fieldname, StoreManager.Type.INT);
    TestState state = register(field);
    StoreClerk clerk = new StoreClerk(state.getStore(), org);
    StoreClerk.Metric metric = clerk.getMetrics().get(0);

    // write some data for that metric
    long ts = get1980();
    File tmp = folder.newFolder("drill");

    Map<String, Object> parquetRow = new HashMap<>();
    parquetRow.put(fieldname, 1);
    Pair<FsSourceTable, File> parquet = writeParquet(state, tmp, org, metrictype, ts,
      parquetRow);

    bootstrapper()
      .withLocalSource(parquet.getKey())
      .bootstrap();

    verifySelectStar(FineoTestUtil.withNext(parquetRow));

    StoreManager manager = new StoreManager(state.getStore());
    manager.updateOrg(org).deleteMetric(metric.getUserName()).commit();

    // now delete the metric and we shouldn't see any data
    try {
      verifySelectStar(r -> {
      });
      fail("Should not be able to read a metric that doesn't exist - the table should be deleted");
    } catch (SQLException e) {
      // expected
    }

    // create a new metric with the same name
    registerSchema(state.getStore(), false, field);

    try {
      verifySelectStar(results -> {
        assertFalse("Got a row after recreating the metric!", results.next());
      });
      fail("should not be able to read when no data present on FS");
    } catch (SQLException e) {
      //expected
    }

    // write a row again
    parquetRow = new HashMap<>();
    parquetRow.put(fieldname, 2);
    Pair<FsSourceTable, File> parquet2 = writeParquet(state, tmp, org, metrictype, ts + 1,
      parquetRow);
    bootstrapper()
      .withLocalSource(parquet.getKey())
      .withLocalSource(parquet2.getKey())
      .bootstrap();
    // this time we should be able to read it
    verifySelectStar(FineoTestUtil.withNext(parquetRow));
  }

  @Test
  public void testMetricDeletionHidingOnDynamo() throws Exception {
    Pair<String, StoreManager.Type> field = p(fieldname, StoreManager.Type.INT);
    TestState state = register(field);
    StoreClerk clerk = new StoreClerk(state.getStore(), org);
    StoreClerk.Metric metric = clerk.getMetrics().get(0);

    // write some data for that metric
    long ts = get1980();

    Map<String, Object> dynamo = prepareItem();
    dynamo.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    dynamo.put(fieldname, 1);
    Table table = state.write(tables.getAsyncClient(), dynamo);

    bootstrapper()
      .withDynamoKeyMapper()
      .withDynamoTable(table)
      .bootstrap();

    verifySelectStar(FineoTestUtil.withNext(dynamo));

    StoreManager manager = new StoreManager(state.getStore());
    manager.updateOrg(org).deleteMetric(metric.getUserName()).commit();

    // now delete the metric and we shouldn't see any data
    try {
      verifySelectStar(r -> {
      });
      fail("Should not be able to read a metric that doesn't exist - the table should be deleted");
    } catch (SQLException e) {
      // expected
    }

    // create a new metric with the same name
    registerSchema(state.getStore(), false, field);

    verifySelectStar(results -> {
      assertFalse("Got a row after recreating the metric!", results.next());
    });

    // write a row again. This goes to the same table, so we don't need to re-bootstrap
    dynamo.put(fieldname, 2);
    state.write(tables.getAsyncClient(), dynamo);

    // this time we should be able to read it
    verifySelectStar(FineoTestUtil.withNext(dynamo));
  }

  private BootstrapFineo.DrillConfigBuilder bootstrapper() {
    return basicBootstrap(new BootstrapFineo().builder());
  }
}
