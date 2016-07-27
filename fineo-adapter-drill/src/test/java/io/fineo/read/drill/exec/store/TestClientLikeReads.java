package io.fineo.read.drill.exec.store;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
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
public class TestClientLikeReads extends BaseFineoTest {

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
