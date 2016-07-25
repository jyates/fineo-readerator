package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import io.fineo.drill.rule.DrillClusterRule;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.ClassRule;
import org.junit.Rule;
import org.schemarepo.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class BaseFineoTest extends BaseDynamoTableTest {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFineoTest.class);

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @Rule
  public TestOutput folder = new TestOutput(false);

  protected final String org = "orgid1", metrictype = "metricid1", fieldname = "field1";
  private static final String DYNAMO_TABLE_PREFIX = "test-dynamo-client-";
  private static final Joiner AND = Joiner.on(" AND ");

  protected Verify<ResultSet> withNext(Map<String, Object>... rows) {
    return result -> {
      for (Map<String, Object> row : rows) {
        assertNext(result, row);
      }
      assertNoMore(result);
    };
  }

  protected void assertNoMore(ResultSet result) throws SQLException {
    assertFalse("Expected no more rows, but got at least one more row!", result.next());
  }

  @FunctionalInterface
  protected interface Verify<T> {
    void verify(T obj) throws SQLException;
  }

  protected String equals(String left, String right) {
    return format("%s = '%s'", left, right);
  }

  protected String verifySelectStar(Verify<ResultSet> verify) throws Exception {
    return verifySelectStar(null, verify);
  }

  protected String verifySelectStar(List<String> wheres, Verify<ResultSet> verify) throws
    Exception {
    String from = format(" FROM fineo.%s.%s", org, metrictype);
    String where = wheres == null ? "" : " WHERE " + AND.join(wheres);
    String stmt = "SELECT *" + from + where + " ORDER BY `timestamp` ASC";
//    String stmt = "SELECT *, field1, *" + from + where;
    verify(stmt, verify);
    return stmt;
  }

  protected void verify(String stmt, Verify<ResultSet> verify) throws Exception {
    LOG.info("Attempting query: " + stmt);
    Connection conn = drill.getConnection();
    verify.verify(conn.createStatement().executeQuery(stmt));
  }

  protected void bootstrap(FsSourceTable... files) throws IOException {
    BootstrapFineo bootstrap = new BootstrapFineo();
    BootstrapFineo.DrillConfigBuilder builder = basicBootstrap(bootstrap.builder());

    for (FsSourceTable file : files) {
      builder.withLocalSource(file);
    }
    assertTrue("Failed to bootstrap drill!", bootstrap.strap(builder));
  }

  protected BootstrapFineo.DrillConfigBuilder basicBootstrap(
    BootstrapFineo.DrillConfigBuilder builder) {
    LocalDynamoTestUtil util = dynamo.getUtil();
    return builder.withLocalDynamo(util.getUrl())
                  .withRepository(tables.getTestTableName())
                  .withOrgs(org)
                  .withCredentials(dynamo.getCredentials().getFakeProvider());
  }

  protected TestState register() throws IOException, OldSchemaException {
    // setup the schema repository
    DynamoDBRepository repository =
      new DynamoDBRepository(ValidatorFactory.EMPTY, tables.getAsyncClient(),
        getCreateTable(tables.getTestTableName()));
    SchemaStore store = new SchemaStore(repository);

    // create a simple schema and store it
    SchemaBuilder.Organization orgSchema =
      SchemaTestUtils.addNewOrg(store, org, metrictype, fieldname);
    Metric metric = (Metric) orgSchema.getSchemas().values().toArray()[0];
    return new TestState(metric, store);
  }

  protected class TestState {
    Metric metric;
    SchemaStore store;
    private DynamoTableCreator creator;

    public TestState(Metric metric, SchemaStore store) {
      this.metric = metric;
      this.store = store;
    }

    FsSourceTable write(File dir, long ts, Map<String, Object>... values)
      throws IOException {
      return write(dir, org, metrictype, ts, values);
    }

    public FsSourceTable write(File dir, String org, String metricType, long ts,
      Map<String, Object>... values) throws IOException {
      return write(dir, org, metricType, ts, newArrayList(values));
    }

    public FsSourceTable write(File dir, String org, String metricType, long ts,
      List<Map<String, Object>> values) throws IOException {
      return writeJson(store, dir, org, metricType, ts, values);
    }

    public Table write(Item wrote) {
      DynamoDB dynamo = new DynamoDB(tables.getAsyncClient());
      if (creator == null) {
        DynamoTableTimeManager ttm = new DynamoTableTimeManager(tables.getAsyncClient(),
          DYNAMO_TABLE_PREFIX);
        this.creator = new DynamoTableCreator(ttm, dynamo, 1, 1);
      }

      long ts = wrote.getLong(Schema.SORT_KEY_NAME);
      String name = creator.getTableAndEnsureExists(ts);
      Table table = dynamo.getTable(name);
      table.putItem(wrote);
      return table;
    }

    public Metric getMetric() {
      return metric;
    }

    public SchemaStore getStore() {
      return store;
    }
  }

  protected static FsSourceTable writeJson(SchemaStore store, File dir, String org,
    String metricType, long ts, List<Map<String, Object>> values) throws IOException {
    return writeJsonAndGetOutputFile(store, dir, org, metricType, ts, values).getKey();
  }

  protected static Pair<FsSourceTable, File> writeJsonAndGetOutputFile(SchemaStore store, File
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

  protected Pair<FsSourceTable, File> writeParquet(TestState state, File dir, String orgid,
    String metricType, long ts, Map<String, Object>... rows) throws Exception {
    // set the values in the row
    StoreClerk clerk = new StoreClerk(state.store, org);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricType);
    for (Map<String, Object> row : rows) {
      setValues(row, orgid, metric, ts);
    }

    // write to a tmp json file
    File tmp = new File(dir, "tmp-json");
    if (!tmp.exists()) {
      assertTrue("Couldn't make the tmp directory: " + tmp, tmp.mkdirs());
    }
    File out = new File(tmp, format("%s-tmp-to-parquet.json", UUID.randomUUID()));
    writeJsonFile(out, rows);

    // create a parquet table
    String path = "dfs.`" + out + "`";
    String table = "tmp_parquet";
    String request = format("CREATE TABLE %s AS SELECT * from %s", table, path);
    String alter = "alter session set `store.format`='parquet'";
    String use = "use dfs.tmp";
    Connection conn = drill.getConnection();
    conn.createStatement().execute(alter);
    conn.createStatement().execute(use);
    conn.createStatement().execute(request);

    //copy the contents to the actual output file that we want to use for ingest
    FsSourceTable source = new FsSourceTable("parquet", dir.getPath());
    File outputDir = createOutputDir(source, metric, ts);
    File from = new File("/tmp", table);
    Files.move(from, outputDir);
    for (File f : outputDir.listFiles()) {
      if (!f.getName().endsWith(".crc")) {
        return new ImmutablePair<>(source, f);
      }
    }
    LOG.error("Could not find a valid parquet file in: " + outputDir);
    return null;
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

  protected static void setValues(Map<String, Object> row, String org, StoreClerk.Metric metric,
    long ts) {
    String metricId = metric.getMetricId();
    row.put(ORG_ID_KEY, org);
    row.put(ORG_METRIC_TYPE_KEY, metricId);
    row.put(TIMESTAMP_KEY, ts);
  }

  protected void assertNext(ResultSet result, Map<String, Object> values) throws SQLException {
    assertNext(0, result, values);
  }

  protected void assertNext(int j, ResultSet result, Map<String, Object> values)
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
              if (expected instanceof byte[]) {
                assertArrayEquals("Mismatch for column: " + key + "\n" + toStringRow(result),
                  (byte[]) expected, (byte[]) actual);
              } else if(expected instanceof BigDecimal){
                // cast the expected down to the
                assertEquals("Mismatch for column: " + key +
                             ".\nExpected:" + values +
                             "\nActual:" + toStringRow(result),
                  expected, actual);
              } else {
                assertEquals("Mismatch for column: " + key +
                             ".\nExpected:" + values +
                             "\nActual:" + toStringRow(result),
                  expected, actual);
              }

            } catch (SQLException e) {
              assertFalse("Got exception: " + e, true);
            }
          });
    List<String> expectedKeys = newArrayList(values.keySet());
    expectedKeys.remove(ORG_ID_KEY);
    expectedKeys.remove(ORG_METRIC_TYPE_KEY);
    expectedKeys.add(FineoCommon.MAP_FIELD);
    List<String> actualKeys = getColumns(result.getMetaData());
    Collections.sort(expectedKeys);
    Collections.sort(actualKeys);
    assertEquals("Wrong number of incoming columns!", expectedKeys, actualKeys);
    assertNull("Radio wasn't null!", result.getObject(FineoCommon.MAP_FIELD));
  }

  protected String toStringRow(ResultSet result) throws SQLException {
    StringBuffer sb = new StringBuffer("row=[");
    ResultSetMetaData meta = result.getMetaData();
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      sb.append(meta.getColumnName(i) + " => " + result.getObject(i) + ",");
    }
    return sb.toString();
  }

  private List<String> getColumns(ResultSetMetaData meta) throws SQLException {
    int count = meta.getColumnCount();
    List<String> cols = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      cols.add(meta.getColumnName(i + 1));
    }
    return cols;
  }

  protected CreateTableRequest getCreateTable(String schemaTable) {
    CreateTableRequest create =
      DynamoDBRepository.getBaseTableCreate(schemaTable);
    create.setProvisionedThroughput(new ProvisionedThroughput()
      .withReadCapacityUnits(1L)
      .withWriteCapacityUnits(1L));
    return create;
  }

  protected long get1980() {
    return LocalDate.of(1980, 1, 1).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
  }
}
