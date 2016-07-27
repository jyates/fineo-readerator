package io.fineo.read.drill.exec.store;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.StoreClerk;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
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
  public void testReadThreeSources() throws Exception {
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

    File tmp = folder.newFolder("drill");
    long tsFile = ts - Duration.ofDays(35).toMillis();

    String field1 = "field1";
    Map<String, Object> parquetRow = new HashMap<>();
    parquetRow.put(field1, true);
    Pair<FsSourceTable, File> parquet = writeParquet(state, tmp, org, metrictype, tsFile,
      parquetRow);

    String uk1 = "uk1";
    Map<String, Object> jsonRow = new HashMap<>();
    jsonRow.put(uk1, 1);
    Pair<FsSourceTable, File> json = FineoTestUtil
      .writeJsonAndGetOutputFile(state.getStore(), tmp, org,
      metrictype, ts, of(jsonRow));
    BootstrapFineo bootstrap = new BootstrapFineo();
    BootstrapFineo.DrillConfigBuilder builder = basicBootstrap(bootstrap.builder())
      // dynamo
      .withDynamoKeyMapper()
      .withDynamoTable(table)
      // fs
      .withLocalSource(parquet.getKey())
      .withLocalSource(json.getKey());
    bootstrap.strap(builder);

    Map<String, Object> row1 = new HashMap<>();
    row1.put(AvroSchemaEncoder.ORG_ID_KEY, org);
    row1.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metrictype);
    row1.put(TIMESTAMP_KEY, tsFile);
    row1.put(field1, true);
    Map<String, Object> radio = new HashMap<>();
    radio.put(uk1, 1);
    row1.put(FineoCommon.MAP_FIELD, radio);

    Map<String, Object> row2 = newHashMap(row1);
    row2.put(TIMESTAMP_KEY, ts);
    row2.remove(FineoCommon.MAP_FIELD);
    verifySelectStar(FineoTestUtil.withNext(row1, row2));
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
