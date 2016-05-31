package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.fasterxml.jackson.jr.ob.JSON;
import io.fineo.drill.rule.DrillClusterRule;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import io.fineo.test.rule.TestOutput;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.schemarepo.ValidatorFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

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

  @Test
  public void test() throws Exception {
    // setup the schema repository
    DynamoDBRepository repository =
      new DynamoDBRepository(ValidatorFactory.EMPTY, tables.getAsyncClient(),
        getCreateTable(tables.getTestTableName()));
    SchemaStore store = new SchemaStore(repository);

    // create a simple schema and store it
    String org = "orgid1", metrictype = "metricid1", fieldname = "field1";
    SchemaTestUtils.addNewOrg(store, org, metrictype, fieldname);

    // write some data in a json file
    Map<String, Object> json = new HashMap<>();
    json.put(AvroSchemaEncoder.ORG_ID_KEY, org);
    json.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metrictype);
    json.put(AvroSchemaEncoder.TIMESTAMP_KEY, 1);
    json.put(fieldname, true);

    File tmp = folder.newFolder("drill");
    File out = new File(tmp, "test.json");
    JSON j = JSON.std;
    j.write(json, out);


    // ensure that the fineo-test plugin is enabled
    LocalDynamoTestUtil util = dynamo.getUtil();
    BootstrapFineo bootstrap = new BootstrapFineo();
    bootstrap.strap(bootstrap.builder()
                             .withLocalDynamo(util.getUrl())
                             .withRepository(tables.getTestTableName())
                             .withLocalSource(tmp));

    try (Connection conn = drill.getConnection()) {
      String from = "FROM fineo.events";
      ResultSet count = conn.createStatement().executeQuery("SELECT * " + from);
      assertTrue(count.next());
      assertEquals("c", count.getString("a"));
    }
  }

  private void setSession(Connection conn, String stmt) throws SQLException {
    conn.createStatement().execute("ALTER SESSION SET " + stmt);
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
