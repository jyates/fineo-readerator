package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import io.fineo.drill.rule.DrillClusterRule;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.junit.ClassRule;
import org.junit.Test;
import org.schemarepo.ValidatorFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestFineoReadTable extends BaseDynamoTableTest {

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

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

    // ensure that the fineo-test plugin is enabled
    LocalDynamoTestUtil util = dynamo.getUtil();
    BootstrapFineo.DrillConfigBuilder
      builder = new BootstrapFineo.DrillConfigBuilder()
      .withLocalDynamo(util.getUrl())
      .withRepository(tables.getTestTableName());
    BootstrapFineo.bootstrap(builder);

    try (Connection conn = drill.getConnection()) {
      String from = "FROM fineo.events";
      ResultSet count = conn.createStatement().executeQuery("SELECT * " + from);
      assertTrue(count.next());
      assertEquals(1, count.getInt(1));
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
