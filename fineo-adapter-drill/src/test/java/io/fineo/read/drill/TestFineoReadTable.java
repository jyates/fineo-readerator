package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.base.Joiner;
import io.fineo.drill.rule.DrillClusterRule;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import io.fineo.test.rule.TestOutput;
import org.apache.avro.Schema;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.schemarepo.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static java.lang.String.format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFineoReadTable extends BaseDynamoTableTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFineoReadTable.class);

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @Rule
  public TestOutput folder = new TestOutput(false);

  private final String org = "orgid1", metrictype = "metricid1", fieldname = "field1";

  /**
   * Store a single row as the 'user visible' name of the field and check that we can read it
   * back as expected
   *
   * @throws Exception on failure
   */
  @Test
  public void testStoringUserVisibleName() throws Exception {
    TestState state = register();

    // write two rows into a json file
    File tmp = folder.newFolder("drill");
    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, false);
    Map<String, Object> values2 = newHashMap(values);
    values.put(fieldname, true);
    List<Map<String, Object>> rows = newArrayList(values, values2);
    File out = state.write(tmp, org, metrictype, 1, rows);

    // ensure that the fineo-test plugin is enabled
    bootstrap(out);

    verifySelectStar(result -> {
      assertNext(result, values);
      LOG.info("----> Row 1 passed!");
      assertNext(result, values2);
    });
  }

  @Test
  public void testStoringNonUserVisibleFieldName() throws Exception {
    TestState state = register();
    // create a new alias name for the field
    Metric metric = state.metric;
    SchemaStore store = state.store;
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.OrganizationBuilder ob = builder.updateOrg(store.getOrgMetadata(org));
    Map<String, String> aliasToCname = AvroSchemaManager.getAliasRemap(metric);
    String cname = aliasToCname.get(fieldname);
    String storeFieldName = "other-field-name";
    SchemaBuilder.Organization org =
      ob.updateSchema(metric).updateField(cname).withAlias(storeFieldName).asField().build()
        .build();
    store.updateOrgMetric(org, metric);

    // write a file with the new field name
    File tmp = folder.newFolder("drill");
    Map<String, Object> values = new HashMap<>();
    values.put(storeFieldName, false);
    File out = state.write(tmp, 1, values);

    bootstrap(out);

    // we should read this as the client visible name
    Boolean value = (Boolean) values.remove(storeFieldName);
    values.put(fieldname, value);

    verifySelectStar(result -> {
      assertNext(result, values);
    });
  }

  @Test
  public void testReadTwoSources() throws Exception {
    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, false);
    Map<String, Object> values2 = newHashMap(values);
    values.put(fieldname, true);

    writeAndReadToIndependentFiles(values, values2);
  }

  /**
   * We don't need to go beyond three sources because this covers 'n' cases of unions over unions.
   */
  @Test
  public void testReadThreeSources() throws Exception {
    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, false);
    Map<String, Object> values2 = newHashMap(values);
    values.put(fieldname, true);
    Map<String, Object> values3 = newHashMap(values);
    values.put(fieldname, false);

    writeAndReadToIndependentFiles(values, values2, values3);
  }


  @Test
  public void testUnknownFieldType() throws Exception {
    TestState state = register();

    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, true);
    String uk = "uk_" + UUID.randomUUID();
    values.put(uk, 1L);
    String uk2 = "uk2_" + UUID.randomUUID();
    values.put(uk2, "hello field 2");

    File tmp = folder.newFolder("drill");
    bootstrap(state.write(tmp, 1, values));

    verifySelectStar(result -> {
      assertTrue(result.next());
      Map radio = (Map) result.getObject(FineoCommon.MAP_FIELD);
      assertEquals(values.get(uk), radio.get(uk));
      assertEquals(values.get(uk2), radio.get(uk2).toString());
    });
  }

  /**
   * We can have a field named _fm, but its stored as an unknown field in the _fm map.
   *
   * @throws Exception on failure
   */
  @Test
  public void testUnknownFieldWithRadioName() throws Exception {
    TestState state = register();

    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, true);
    values.put(FineoCommon.MAP_FIELD, 1L);

    File tmp = folder.newFolder("drill");
    bootstrap(state.write(tmp, 1, values));

    verifySelectStar(result -> {
      assertTrue(result.next());
      Map radio = (Map) result.getObject(FineoCommon.MAP_FIELD);
      assertEquals(values.get(FineoCommon.MAP_FIELD), radio.get(FineoCommon.MAP_FIELD));
    });
  }

  @Test
  public void testFilterOnUnknownField() throws Exception {
    TestState state = register();

    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, true);
    String uk = "uk_" + UUID.randomUUID();
    values.put(uk, 1L);

    File tmp = folder.newFolder("drill");
    bootstrap(state.write(tmp, 1, values));

    // definitely doesn't match
    String field = FineoCommon.MAP_FIELD + "['" + uk + "']";
    verifySelectStar(of(equals(field, "2")), result -> {
      assertFalse(result.next());
      System.out.println(result);
    });

    // matching case
    verifySelectStar(of(equals(field, Long.toString(1L))), result -> {
      assertTrue(result.next());
      Map radio = (Map) result.getObject(FineoCommon.MAP_FIELD);
      assertEquals(values.get(uk), radio.get(uk));
    });
  }

  @Test
  public void testSupportedFieldTypes() throws Exception {
    Map<String, Object> values = bootstrapFileWithFields(
      f(true, Schema.Type.BOOLEAN),
      f(new byte[]{1}, Schema.Type.BYTES),
      f(2.0, Schema.Type.DOUBLE),
      f(3.0f, Schema.Type.FLOAT),
      f(4, Schema.Type.INT),
      f(5L, Schema.Type.LONG),
      f("6string", Schema.Type.STRING));

//    verify("SELECT *, CAST(f4 as FLOAT) FROM fineo."+org+"."+metrictype, result ->{});
    verifySelectStar(result -> assertNext(result, values));
  }

  @Test
  public void testSimpleCast() throws Exception {
    Map<String, Object> values = bootstrapFileWithFields(
      f(4, Schema.Type.FLOAT));
    values.put("f0", 4.0f);
    verifySelectStar(result -> assertNext(result, values));
  }

  @Test
  public void testCastWithMultipleFieldAliases() throws Exception {
    DynamoDBRepository repository =
      new DynamoDBRepository(ValidatorFactory.EMPTY, tables.getAsyncClient(),
        getCreateTable(tables.getTestTableName()));
    SchemaStore store = new SchemaStore(repository);
    StoreManager manager = new StoreManager(store);
    StoreManager.MetricBuilder builder = manager.newOrg(org)
                                                .newMetric().setDisplayName(metrictype);
    builder.newField().withName("f0").withType(Schema.Type.FLOAT.getName()).withAliases(of("af0"))
           .build().build().commit();

    Map<String, Object> values = new HashMap<>();
    values.put("af0", 4);

    File tmp = folder.newFolder("drill");
    bootstrap(writeJson(store, tmp, org, metrictype, 1, of(values)));

    values.remove("af0");
    values.put("f0", 4.0f);
    verifySelectStar(result -> assertNext(result, values));
  }


  /**
   * Write bytes json row and read it back in as bytes. This is an issue because bytes are
   * mis-mapped from json as varchar
   *
   * @throws Exception
   */
  @Test
  public void testBytesTypeRemap() throws Exception {
    Map<String, Object> values = bootstrapFileWithFields(f(new byte[]{1}, Schema.Type.BYTES));
    verifySelectStar(result -> assertNext(result, values));
  }

  private Map<String, Object> bootstrapFileWithFields(FieldInstance<?>... fields)
    throws IOException, OldSchemaException {
    // setup the schema repository
    DynamoDBRepository repository =
      new DynamoDBRepository(ValidatorFactory.EMPTY, tables.getAsyncClient(),
        getCreateTable(tables.getTestTableName()));
    SchemaStore store = new SchemaStore(repository);
    StoreManager manager = new StoreManager(store);
    StoreManager.MetricBuilder builder = manager.newOrg(org)
                                                .newMetric().setDisplayName(metrictype);
    Map<String, Object> values = new HashMap<>();
    for (int i = 0; i < fields.length; i++) {
      String name = "f" + i;
      FieldInstance<?> field = fields[i];
      builder.newField().withName(name).withType(field.type.getName()).build();
      values.put(name, field.inst);
    }
    builder.build().commit();

    File tmp = folder.newFolder("drill");
    bootstrap(writeJson(store, tmp, org, metrictype, 1, of(values)));

    return values;
  }

  private class FieldInstance<T> {
    private final T inst;
    private final Schema.Type type;

    public FieldInstance(T inst, Schema.Type type) {
      this.inst = inst;
      this.type = type;
    }
  }

  private <T> FieldInstance<T> f(T inst, Schema.Type type) {
    return new FieldInstance<>(inst, type);
  }

  private void writeAndReadToIndependentFiles(Map<String, Object>... fileContents)
    throws Exception {
    TestState state = register();

    // write two rows into a json file
    File tmp = folder.newFolder("drill");
    List<File> files = new ArrayList<>();
    int i = 0;
    for (Map<String, Object> contents : fileContents) {
      files.add(state.write(tmp, i++, contents));
    }

    // ensure that the fineo-test plugin is enabled
    bootstrap(files.toArray(new File[0]));

    verifySelectStar(result -> {
      int j = 0;
      for (Map<String, Object> content : fileContents) {
        LOG.info("Checking row " + j + ".\n\tExpected Content =>" + content);
        assertNext(result, content);
      }
    });
  }

  private static final Joiner AND = Joiner.on(" AND ");

  private void verifySelectStar(Verify<ResultSet> verify) throws Exception {
    verifySelectStar(null, verify);
  }

  private void verifySelectStar(List<String> wheres, Verify<ResultSet> verify) throws Exception {
    String from = format(" FROM fineo.%s.%s", org, metrictype);
    String where = wheres == null ? "" : " WHERE " + AND.join(wheres);
    String stmt = "SELECT *" + from + where + " ORDER BY `timestamp` ASC";
//    String stmt = "SELECT *, field1, *" + from + where;
    verify(stmt, verify);
  }

  private void verify(String stmt, Verify<ResultSet> verify) throws Exception {
    LOG.info("Attempting query: " + stmt);
    Connection conn = drill.getConnection();
    verify.verify(conn.createStatement().executeQuery(stmt));
  }


  private String equals(String left, String right) {
    return format("%s = '%s'", left, right);
  }

  @FunctionalInterface
  private interface Verify<T> {
    void verify(T obj) throws SQLException;
  }

  private void bootstrap(File... files) throws IOException {
    LocalDynamoTestUtil util = dynamo.getUtil();
    BootstrapFineo bootstrap = new BootstrapFineo();
    BootstrapFineo.DrillConfigBuilder builder =
      bootstrap.builder()
               .withLocalDynamo(util.getUrl())
               .withRepository(tables.getTestTableName())
               .withOrgs(org);
    for (File file : files) {
      builder.withLocalSource(file);
    }
    bootstrap.strap(builder);
  }

  private TestState register() throws IOException, OldSchemaException {
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

  private class TestState {
    private Metric metric;
    private SchemaStore store;

    public TestState(Metric metric, SchemaStore store) {
      this.metric = metric;
      this.store = store;
    }

    private File write(File dir, long ts, Map<String, Object> values)
      throws IOException {
      return write(dir, org, metrictype, ts, newArrayList(values));
    }

    private File write(File dir, String org, String metricType, long ts,
      List<Map<String, Object>> values) throws IOException {
      return writeJson(store, dir, org, metricType, ts, values);
    }
  }

  private static File writeJson(SchemaStore store, File dir, String org, String metricType, long ts,
    List<Map<String, Object>> values) throws IOException {
    StoreClerk clerk = new StoreClerk(store, org);
    for (Map<String, Object> json : values) {
      json.put(ORG_ID_KEY, org);
      // get the actual org type
      StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricType);
      json.put(ORG_METRIC_TYPE_KEY, metric.getMetricId());
      json.put(TIMESTAMP_KEY, ts);
    }

    File out = new File(dir, format("test-%s-%s.json", ts, UUID.randomUUID()));
    LOG.info("Using input file: " + out);
    JSON j = JSON.std;
    j.write(values, out);
    return out;
  }

  private void assertNext(ResultSet result, Map<String, Object> values) throws SQLException {
    assertTrue("Could not get next result for values: " + values, result.next());
    values.keySet().stream()
          .filter(Predicate.isEqual(AvroSchemaEncoder.ORG_ID_KEY).negate())
          .filter(Predicate.isEqual(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY).negate())
          .forEach(key -> {
            try {
              Object expected = values.get(key);
              Object actual = result.getObject(key);
              if (expected instanceof byte[]) {
                assertArrayEquals("Mismatch for column: " + key, (byte[]) expected,
                  (byte[]) actual);
              } else {
                assertEquals("Mismatch for column: " + key, expected, actual);
              }

            } catch (SQLException e) {
              assertFalse("Got exception: " + e, true);
            }
          });
    List<String> expectedKeys = newArrayList(values.keySet());
    expectedKeys.remove(AvroSchemaEncoder.ORG_ID_KEY);
    expectedKeys.remove(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY);
    expectedKeys.add(FineoCommon.MAP_FIELD);
    List<String> actualKeys = getColumns(result.getMetaData());
    Collections.sort(expectedKeys);
    Collections.sort(actualKeys);
    assertEquals("Wrong number of incoming columns!", expectedKeys, actualKeys);
  }

  private List<String> getColumns(ResultSetMetaData meta) throws SQLException {
    int count = meta.getColumnCount();
    List<String> cols = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      cols.add(meta.getColumnName(i + 1));
    }
    return cols;
  }

  private CreateTableRequest getCreateTable(String schemaTable) {
    CreateTableRequest create =
      DynamoDBRepository.getBaseTableCreate(schemaTable);
    create.setProvisionedThroughput(new ProvisionedThroughput()
      .withReadCapacityUnits(1L)
      .withWriteCapacityUnits(1L));
    return create;
  }
}
