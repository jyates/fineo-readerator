package io.fineo.read.drill.exec.store;

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
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.fineo.drill.exec.store.dynamo.DynamoPlanValidationUtils.lte;
import static io.fineo.read.drill.FineoTestUtil.bt;
import static io.fineo.read.drill.FineoTestUtil.get1980;
import static io.fineo.read.drill.FineoTestUtil.p;
import static io.fineo.read.drill.FineoTestUtil.withNext;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static org.apache.calcite.util.ImmutableNullableList.of;

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
    TestState state = register();
    long ts = get1980();

    Item wrote = prepareItem(state);
    wrote.with(Schema.SORT_KEY_NAME, ts);
    wrote.with("field1", true);
    Table table = state.write(wrote);

    File tmp = folder.newFolder("drill");
    long tsFile = ts - Duration.ofDays(35).toMillis();

    String field1 = "field1";
    Map<String, Object> parquetRow = new HashMap<>();
    parquetRow.put(field1, false);
    Pair<FsSourceTable, File> parquet = writeParquet(state, tmp, org, metrictype, tsFile,
      parquetRow);

    bootstrapper()
      // dynamo
      .withDynamoKeyMapper()
      .withDynamoTable(table)
      // fs
      .withLocalSource(parquet.getKey())
      .bootstrap();

    Map<String, Object> dynamoRow = new HashMap<>();
    dynamoRow.put(AvroSchemaEncoder.ORG_ID_KEY, org);
    dynamoRow.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metrictype);
    dynamoRow.put(TIMESTAMP_KEY, ts);
    dynamoRow.put(field1, true);

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
    Item dynamo = prepareItem(state);
    dynamo.with(Schema.SORT_KEY_NAME, ts);
    dynamo.with(fieldname, 1);
    Table table = state.write(dynamo);

    Map<String, Object> parquet = new HashMap<>();
    parquet.put(fieldname, 2);
    File drill = folder.newFolder("drill");
    FsSourceTable source = state.writeParquet(drill, ts, parquet);
    bootstrapper().withDynamoKeyMapper().withDynamoTable(table).withLocalSource(source).bootstrap();

    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaEncoder.ORG_ID_KEY, org);
    expected.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(TIMESTAMP_KEY, ts);
    expected.put(fieldname, 1);
    verifySelectStar(withNext(expected));
  }

  @Test
  public void testPruneFileDirectoryAndDynamo() throws Exception {
    TestState state = register(p(fieldname, StoreManager.Type.INT));
    long ts = get1980();
    Item dynamo = prepareItem(state);
    dynamo.with(Schema.SORT_KEY_NAME, ts);
    dynamo.with(fieldname, 1);
    Table table = state.write(dynamo);

    Map<String, Object> parquet = new HashMap<>();
    parquet.put(fieldname, 2);

    File drillDir = folder.newFolder("drill");
    Pair<FsSourceTable, File> source = writeParquet(state, drillDir, org, metrictype, ts, parquet);
    BootstrapFineo.DrillConfigBuilder builder = bootstrapper().withDynamoKeyMapper()
                                                              .withDynamoTable(table)
                                                              .withLocalSource(source.getKey());
    // write some parquet data in the future
    Map<String, Object> parquet2 = new HashMap<>();
    parquet.put(fieldname, 3);
    state.writeParquet(drillDir, ts + ONE_DAY_MILLIS * 35, parquet2);

    builder.bootstrap();

    Map<String, Object> expected = new HashMap<>();
    expected.put(AvroSchemaEncoder.ORG_ID_KEY, org);
    expected.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metrictype);
    expected.put(TIMESTAMP_KEY, ts);
    expected.put(fieldname, 1);
    String query = verifySelectStar(of(bt(AvroSchemaEncoder.TIMESTAMP_KEY) + " <= " + ts), withNext
      (expected));

    // validate that we only read the single parquet that we expected and the dynamo table
    DynamoFilterSpec keyFilter = DynamoPlanValidationUtils.equals(Schema.PARTITION_KEY_NAME,
      dynamo.getString(Schema.PARTITION_KEY_NAME)).and(lte(Schema.SORT_KEY_NAME, ts));
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

  private Item prepareItem(TestState state) throws SchemaNotFoundException {
    StoreClerk clerk = new StoreClerk(state.getStore(), org);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metrictype);

    Item wrote = new Item();
    wrote.with(Schema.PARTITION_KEY_NAME, org + metric.getMetricId());
    return wrote;
  }

  private BootstrapFineo.DrillConfigBuilder bootstrapper() {
    return basicBootstrap(new BootstrapFineo().builder());
  }
}
