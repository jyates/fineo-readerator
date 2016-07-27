package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.fasterxml.jackson.jr.ob.JSON;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import oadd.org.apache.drill.exec.util.Text;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static java.lang.String.format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FineoTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(FineoTestUtil.class);

  private FineoTestUtil(){}

  protected static void setValues(Map<String, Object> row, String org, StoreClerk.Metric metric,
    long ts) {
    String metricId = metric.getMetricId();
    row.put(ORG_ID_KEY, org);
    row.put(ORG_METRIC_TYPE_KEY, metricId);
    row.put(TIMESTAMP_KEY, ts);
  }

  protected static void assertNext(int j, ResultSet result, Map<String, Object> values)
    throws SQLException {
    assertTrue("Could not get next result for values: " + values, result.next());
    if (j >= 0) {
      String row = toStringRow(result);
      LOG.info("Checking row " + j + "." +
                             "\n\tExpected Content =>" + values +
                             "\n\tActual row content: " + row);
    }
    values.keySet().stream()
          .filter(Predicate.isEqual(ORG_ID_KEY).negate())
          .filter(Predicate.isEqual(ORG_METRIC_TYPE_KEY).negate())
          .forEach(key -> {
            try {
              Object expected = values.get(key);
              Object actual = result.getObject(key);
              assertSqlValuesEquals(key, result, expected, actual);
            } catch (SQLException e) {
              assertFalse("Got exception: " + e, true);
            }
          });
    List<String> expectedKeys = newArrayList(values.keySet());
    expectedKeys.remove(ORG_ID_KEY);
    expectedKeys.remove(ORG_METRIC_TYPE_KEY);
    List<String> actualKeys = getColumns(result.getMetaData());
    Collections.sort(expectedKeys);
    Collections.sort(actualKeys);
    assertEquals("Wrong number of incoming columns!", expectedKeys, actualKeys);
  }

  private static void assertSqlValuesEquals(String key, ResultSet result, Object expected, Object
    actual)
    throws SQLException {
    if (expected instanceof byte[]) {
      assertArrayEquals("Mismatch for column: " + key + "\n" + toStringRow(result),
        (byte[]) expected, (byte[]) actual);
      return;
    } else if (expected instanceof BigDecimal) {
      // cast the expected down to the
      assertEquals("Mismatch for column: " + key + "\nActual:" + toStringRow(result),
        expected, actual);
      return;
    } else if (expected instanceof Map) {
      assertTrue(actual instanceof Map);
      Map expectedMap = (Map) expected;
      Map actualMap = (Map) actual;
      for (Object entry : expectedMap.entrySet()) {
        Object mapKey = ((Map.Entry) entry).getKey();
        assertSqlValuesEquals(key + "." + mapKey, result, ((Map.Entry) entry).getValue(), actualMap
          .get(mapKey));
      }
      return;
    } else if (expected instanceof String) {
      if (actual instanceof Text) {
        assertSqlValuesEquals(key, result, expected, actual.toString());
        return;
      }
      // fall through
    }
    assertEquals("Mismatch for column: " + key + "\nActual:" + toStringRow(result),
      expected, actual);
  }

  protected static String toStringRow(ResultSet result) throws SQLException {
    StringBuffer sb = new StringBuffer("row=[");
    ResultSetMetaData meta = result.getMetaData();
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      sb.append(meta.getColumnName(i) + " => " + result.getObject(i) + ",");
    }
    return sb.toString();
  }

  private static List<String> getColumns(ResultSetMetaData meta) throws SQLException {
    int count = meta.getColumnCount();
    List<String> cols = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      cols.add(meta.getColumnName(i + 1));
    }
    return cols;
  }

  public static String bt(String columnName) {
    return format("`%s`", columnName);
  }

  public static long get1980() {
    return LocalDate.of(1980, 1, 1).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
  }

  public static CreateTableRequest getCreateTable(String schemaTable) {
    CreateTableRequest create =
      DynamoDBRepository.getBaseTableCreate(schemaTable);
    create.setProvisionedThroughput(new ProvisionedThroughput()
      .withReadCapacityUnits(1L)
      .withWriteCapacityUnits(1L));
    return create;
  }

  protected static File createOutputDir(FsSourceTable table, StoreClerk.Metric metric, long ts) {
    String metricId = metric.getMetricId();
    File dir = new File(table.getBasedir());
    File version = new File(dir, FineoStoragePlugin.VERSION);
    File format = new File(version, table.getFormat());
    File orgDir = new File(format, metric.getOrgId());
    File metricDir = new File(orgDir, metricId);
    Date date = new Date(ts);
    File dateDir = new File(metricDir, date.toString());
    if (!dateDir.exists()) {
      assertTrue("Couldn't make output directory! Dir: " + dateDir, dateDir.mkdirs());
    }
    return dateDir;
  }

  protected static void writeJsonFile(File out, Object toWrite) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(out);
         BufferedOutputStream bos = new BufferedOutputStream(fos)) {
      LOG.info("Using input file: " + out);
      JSON j = JSON.std;
      j.write(toWrite, bos);
    }
  }

  public static FsSourceTable writeJson(SchemaStore store, File dir, String org,
    String metricType, long ts, List<Map<String, Object>> values) throws IOException {
    return writeJsonAndGetOutputFile(store, dir, org, metricType, ts, values).getKey();
  }

  public static Pair<FsSourceTable, File> writeJsonAndGetOutputFile(SchemaStore store, File
    dir, String org, String metricType, long ts, List<Map<String, Object>> values)
    throws IOException {
    StoreClerk clerk = new StoreClerk(store, org);

    // get the actual metric type
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricType);

    FsSourceTable table = new FsSourceTable("json", dir.getPath());
    File outputDir = createOutputDir(table, metric, ts);

    for (Map<String, Object> json : values) {
      setValues(json, org, metric, ts);
    }

    // actually write the events
    File out = new File(outputDir, format("test-%s-%s.json", ts, UUID.randomUUID()));
    writeJsonFile(out, values);
    return new ImmutablePair<>(table, out);
  }

  public static BaseFineoTest.Verify<ResultSet> withNext(Map<String, Object>... rows) {
    return result -> {
      int i = 0;
      for (Map<String, Object> row : rows) {
        assertNext(i++, result, row);
      }
      assertNoMore(result);
    };
  }

  protected static void assertNoMore(ResultSet result) throws SQLException {
    assertFalse("Expected no more rows, but got at least one more row!", result.next());
  }
}
