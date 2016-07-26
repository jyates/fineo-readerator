package io.fineo.read.drill.exec.store;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.StoreClerk;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Maps.newHashMap;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static org.apache.calcite.util.ImmutableNullableList.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Validate reads across all three sources with different varying time ranges. Things that need
 * covering are:
 * - ensuring that we don't have duplicate data when reading overlapping time ranges
 * - combining fields across json and parquet formats
 */
public class TestClientLikeReads extends BaseFineoTest {

  @Test
  public void testChangingUnknownFieldsAcrossRows() throws Exception {
    TestState state = register();

    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, true);
    String uk = "uk";
    values.put(uk, 1L);
    String uk2 = "uk2";
    values.put(uk2, "hello field");

    File tmp = folder.newFolder("drill");
    FsSourceTable f1 = state.write(tmp, 1, values);
    // build the expected values
    values.remove(uk);
    values.remove(uk2);
    addRadio(values, p(uk, 1L), p(uk2, "hello field"));

    Map<String, Object> values2 = new HashMap<>();
    values2.put(fieldname, false);
    String uk3 = "uk3";
    values2.put(uk3, true);

    bootstrap(f1, state.write(tmp, 2, values2));
//    bootstrap(state.write(tmp, 2, values2));

    values2.remove(uk3);
    addRadio(values2, p(uk3, true));

    verifySelectStar(withNext(values, values2));
//      verifySelectStar(withNext(values2));
  }

  private <T, V> Pair<T, V> p(T t, V v) {
    return new ImmutablePair<>(t, v);
  }

  private void addRadio(Map<String, Object> map, Pair<String, Object>... values) {
    Map<String, Object> radio = new HashMap<>();
    for (Pair<String, Object> p : values) {
      radio.put(p.getKey(), p.getValue());
    }
    map.put(FineoCommon.MAP_FIELD, radio);
  }

  @Test
  public void testReadThreeSources() throws Exception {
    TestState state = register();
    long ts = get1980();

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
    Pair<FsSourceTable, File> json = writeJsonAndGetOutputFile(state.getStore(), tmp, org,
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
    verifySelectStar(withNext(row1, row2));
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
