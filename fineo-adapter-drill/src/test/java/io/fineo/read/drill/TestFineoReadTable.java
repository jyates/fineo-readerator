package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.fasterxml.jackson.jr.ob.JSON;
import io.fineo.drill.rule.DrillClusterRule;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import io.fineo.test.rule.TestOutput;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.schemarepo.ValidatorFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestFineoReadTable extends BaseDynamoTableTest {

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
    register();

    // write two rows into a json file
    File tmp = folder.newFolder("drill");
    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, false);
    Map<String, Object> values2 = newHashMap(values);
    values.put(fieldname, true);
    List<Map<String, Object>> rows = newArrayList(values, values2);
    File out = write(tmp, org, metrictype, 1, rows);

    // ensure that the fineo-test plugin is enabled
    bootstrap(out);

    verifySelectStar(result -> {
      assertNext(result, values);
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
    File out = write(tmp, 1, values);

    bootstrap(out);

    // we should read this as the client visible name
    Boolean value = (Boolean) values.remove(storeFieldName);
    values.put(fieldname, value);

    verifySelectStar(result -> {
      assertNext(result, values);
    });
  }

  private void verifySelectStar(Verify<ResultSet> verify) throws Exception {
    try (Connection conn = drill.getConnection()) {
      String from = " FROM fineo.events";
      String where = String
        .format(" WHERE %s = '%s' AND %s = '%s'",
          ORG_ID_KEY, org,
          ORG_METRIC_TYPE_KEY, metrictype);
      String stmt = "SELECT *" + from + where;
      verify.verify(conn.createStatement().executeQuery(stmt));
    }
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
               .withRepository(tables.getTestTableName());
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
  }

  private void assertNext(ResultSet result, Map<String, Object> values) throws SQLException {
    assertTrue(result.next());
    for (Map.Entry<String, Object> e : values.entrySet()) {
      assertEquals(e.getValue(), result.getObject(e.getKey()));
    }
  }

  private File write(File dir, long ts, Map<String, Object> values)
    throws IOException {
    return write(dir, org, metrictype, ts, newArrayList(values));
  }

  private File write(File dir, String org, String metricType, long ts,
    List<Map<String, Object>> values) throws IOException {
    for (Map<String, Object> json : values) {
      json.put(ORG_ID_KEY, org);
      json.put(ORG_METRIC_TYPE_KEY, metricType);
      json.put(TIMESTAMP_KEY, ts);
    }

    File out = new File(dir, format("test-%s-%s.json", ts, UUID.randomUUID()));
    JSON j = JSON.std;
    j.write(values, out);
    return out;
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
