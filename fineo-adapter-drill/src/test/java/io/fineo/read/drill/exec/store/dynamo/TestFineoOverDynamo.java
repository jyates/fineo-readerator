package io.fineo.read.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.avro.Schema;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.schema.avro.AvroSchemaEncoder;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Do Fineo-style reads over a dynamo table
 */
public class TestFineoOverDynamo extends BaseFineoTest {

  @Test
  public void testReadSingleRow() throws Exception {
    TestState state = register();
    long ts = get1980();

    Item wrote = new Item();
    wrote.with(Schema.PARTITION_KEY_NAME, org + metrictype);
    wrote.with(Schema.SORT_KEY_NAME, ts);
    wrote.with("field1", 10);
    Table table = state.write(wrote);
    bootstrap(table);

    Item expected = new Item();
    expected.with(AvroSchemaEncoder.ORG_ID_KEY, org);
    expected.with(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metrictype);
    expected.with(AvroSchemaEncoder.TIMESTAMP_KEY, ts);
    expected.with("field1", 10);
    verifySelectStar(withNext(expected.asMap()));
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
