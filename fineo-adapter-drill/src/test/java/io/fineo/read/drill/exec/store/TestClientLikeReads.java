package io.fineo.read.drill.exec.store;

import com.amazonaws.services.dynamodbv2.document.Table;
import io.fineo.drill.ClusterTest;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoQueryFilterSpec;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.read.drill.PlanValidator;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.read.drill.fs.BaseFineoTestWithErrorReads;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.drill.exec.store.dynamo.DynamoPlanValidationUtils.lte;
import static io.fineo.read.drill.FineoTestUtil.bt;
import static io.fineo.read.drill.FineoTestUtil.get1980;
import static io.fineo.read.drill.FineoTestUtil.p;
import static io.fineo.read.drill.FineoTestUtil.withNext;
import static io.fineo.read.drill.exec.store.dynamo.TestFineoOverDynamo.getFilterSpec;
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
public class TestClientLikeReads extends BaseFineoTestWithErrorReads {

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
    Table table = state.writeToDynamo(wrote);

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
    Table table = state.writeToDynamo(dynamo);

    Map<String, Object> parquet = new HashMap<>();
    parquet.put(fieldname, 2);
    File drill = folder.newFolder("drill");
    FsSourceTable source = writeParquet(state, drill, ts, parquet);
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
    Table table = state.writeToDynamo(dynamo);

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
    writeParquet(state, drillDir, ts + ONE_DAY_MILLIS * 35, parquet2);

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
    DynamoFilterSpec keyFilter = getFilterSpec(state.getStore(), org, metrictype)
      .and(lte(Schema.SORT_KEY_NAME, ts));
    validateAsOrgUser(
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
      .done());
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
    FsSourceTable out = writeParquet(state, drill, 1, values);

    bootstrap(out);

    // we should read this as the client visible name
    Boolean value = (Boolean) values.remove(storeFieldName);
    values.put(fieldname, value);

    verifySelectStar(FineoTestUtil.withNext(values));
  }

  @Test
  public void testStoredNumericalFieldBeforeSchemaCreatedButReadableAfter() throws Exception {
    TestState state = register();

    // write a new event with the field
    String storeFieldName = "other-field-name";
    Map<String, Object> values = prepareItem();
    values.put(Schema.SORT_KEY_NAME, get1980());
    values.put(storeFieldName, 1);
    Table table = state.writeToDynamo(values);

    bootstrapper()
      // dynamo
      .withDynamoKeyMapper()
      .withDynamoTable(table)
      .bootstrap();

    // add the schema for the row
    SchemaStore store = state.getStore();
    StoreManager manager = new StoreManager(store);
    manager.updateOrg(org)
           .updateMetric(metrictype)
           .newField().withType(StoreManager.Type.INTEGER).withName(storeFieldName).build()
           .build()
           .commit();

    values.put(fieldname, null);
    verifySelectStar(FineoTestUtil.withNext(values));
  }

  @Test
  public void testMissingValueForFieldInOldWrite() throws Exception {
    TestState state = register();

    String storeFieldName = "late-added-field";
    Map<String, Object> values = prepareItem();
    values.put(Schema.SORT_KEY_NAME, get1980());
    values.put(fieldname, true);
    Table table = state.writeToDynamo(values);

    Map<String, Object> values2 = prepareItem();
    values2.put(Schema.SORT_KEY_NAME, get1980() + 1);
    values2.put(storeFieldName, 1);
    values2.put(fieldname, true);
    // same table as above, so skip adding it. It actually causes an error if we add it again.
    // TODO #startup figure out why we can't add the same table again in bootstrap
    state.writeToDynamo(values2);

    bootstrapper()
      // dynamo
      .withDynamoKeyMapper()
      .withDynamoTable(table)
      .bootstrap();

    // add the schema for the row
    SchemaStore store = state.getStore();
    StoreManager manager = new StoreManager(store);
    manager.updateOrg(org)
           .updateMetric(metrictype)
           .newField().withType(StoreManager.Type.INTEGER).withName(storeFieldName).build()
           .build()
           .commit();

    values.put(fieldname, true);
    values.put(storeFieldName, null);
    verifySelectStar(FineoTestUtil.withNext(values, values2));
  }

  @Test
  public void testReadOnlyNonNullValueForField() throws Exception {
    TestState state = register();

    String storeFieldName = "field-name", aliasName = "alias-field-name";
    Map<String, Object> values = prepareItem();
    values.put(Schema.SORT_KEY_NAME, get1980());
    values.put(fieldname, true);
    values.put(storeFieldName, 1);
    Table table = state.writeToDynamo(values);

    Map<String, Object> values2 = prepareItem();
    values2.put(Schema.SORT_KEY_NAME, get1980() + 1);
    values2.put(fieldname, true);
    values2.put(aliasName, 2);
    state.writeToDynamo(values2);

    bootstrapper()
      // dynamo
      .withDynamoKeyMapper()
      .withDynamoTable(table)
      .bootstrap();

    // add the schema for the row
    SchemaStore store = state.getStore();
    StoreManager manager = new StoreManager(store);
    manager.updateOrg(org)
           .updateMetric(metrictype)
           .newField()
           .withType(StoreManager.Type.INTEGER)
           .withName(storeFieldName)
           .withAliases(newArrayList(aliasName)).build().build().commit();

    values2.remove(aliasName);
    values2.put(storeFieldName, 2);
    verifySelectStar(FineoTestUtil.withNext(values, values2));
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

    // writeToDynamo some data for that metric
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

    // writeToDynamo a row again
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

    // writeToDynamo some data for that metric
    long ts = get1980();

    Map<String, Object> dynamo = prepareItem();
    dynamo.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    dynamo.put(fieldname, 1);
    Table table = state.writeToDynamo(dynamo);

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

    // writeToDynamo a row again. This goes to the same table, so we don't need to re-bootstrap
    dynamo.put(fieldname, 2);
    state.writeToDynamo(dynamo);

    // this time we should be able to read it
    verifySelectStar(FineoTestUtil.withNext(dynamo));
  }

  @Test
  @Ignore("Deleting fields not yet supported. This is how we would validate that it works")
  public void testDeleteFieldInMetric() throws Exception {
    Pair<String, StoreManager.Type> field = p(fieldname, StoreManager.Type.INT);
    TestState state = register(field);
    StoreClerk clerk = new StoreClerk(state.getStore(), org);
    StoreClerk.Metric metric = clerk.getMetrics().get(0);

    // writeToDynamo some data for that metric
    long ts = get1980();

    Map<String, Object> dynamo = prepareItem();
    dynamo.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    dynamo.put(fieldname, 1);
    Table table = state.writeToDynamo(dynamo);

    bootstrapper()
      .withDynamoKeyMapper()
      .withDynamoTable(table)
      .bootstrap();

    verifySelectStar(FineoTestUtil.withNext(dynamo));

    // now delete the field and we shouldn't see any data
    StoreManager manager = new StoreManager(state.getStore());
    manager.updateOrg(org)
           .updateMetric(metric.getUserName()).deleteField(fieldname).build()
           .commit();
    verifyNoRows();

    //add a new field, write some data and then we should be able to read the new data
    manager.updateOrg(org)
           .updateMetric(metric.getUserName())
           .newField().withName(fieldname).withType(StoreManager.Type.INTEGER).build()
           .build().commit();

    // writeToDynamo a row again. This goes to the same table, so we don't need to re-bootstrap
    dynamo.put(fieldname, 2);
    state.writeToDynamo(dynamo);

    // this time we should be able to read it, but not the old row
    verifySelectStar(FineoTestUtil.withNext(dynamo));
  }

  private void verifyNoRows() throws Exception {
    verifySelectStar(withNext());
  }
}
