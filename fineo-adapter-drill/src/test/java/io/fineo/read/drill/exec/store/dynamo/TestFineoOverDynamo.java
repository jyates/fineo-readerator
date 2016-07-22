package io.fineo.read.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Table;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.BootstrapFineo;
import org.junit.Test;

import java.io.IOException;

/**
 * Do Fineo-style reads over a dynamo table
 */
public class TestFineoOverDynamo extends BaseFineoTest {

  @Test
  public void testReadSingleRow() throws Exception {
    Table table = null;
    bootstrap(table);
  }

  private void bootstrap(Table... tables) throws IOException {
    LocalDynamoTestUtil util = dynamo.getUtil();
    BootstrapFineo bootstrap = new BootstrapFineo();
    BootstrapFineo.DrillConfigBuilder builder =
      bootstrap.builder()
               .withLocalDynamo(util.getUrl())
               .withRepository(this.tables.getTestTableName())
               .withOrgs(org)
               .withDynamoKeyMapper();
    for (Table table : tables) {
      builder.withDynamoTable(table);
    }
    bootstrap.strap(builder);
  }
}
