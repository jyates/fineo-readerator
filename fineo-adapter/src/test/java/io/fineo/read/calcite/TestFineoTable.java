package io.fineo.read.calcite;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.schema.MapRecord;
import io.fineo.schema.Record;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.schemarepo.ValidatorFactory;

import java.util.HashMap;
import java.util.Map;

import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.create;
import static java.lang.String.format;
import static org.junit.Assert.assertFalse;

/**
 *
 */
public class TestFineoTable extends BaseDynamoTableTest {

  @Test
  public void testSelectStar() throws Exception {
    // setup the schema repository
    DynamoDBRepository repository =
      new DynamoDBRepository(ValidatorFactory.EMPTY, tables.getAsyncClient(),
        getCreateTable(tables.getTestTableName()));
    SchemaStore store = new SchemaStore(repository);

    // create a simple schema
    String org = "orgid1", metric = "metricid2", field = "field1";
    SchemaTestUtils.addNewOrg(store, org, metric, field);

    // write some data to the primary table
    Map<String, Object> map = new HashMap<>();
    long timestamp = System.currentTimeMillis();
    map.put(TIMESTAMP_KEY, timestamp);
    map.put(ORG_ID_KEY, org);
    map.put(ORG_METRIC_TYPE_KEY, metric);
    map.put(field, "true");
    Record record = new MapRecord(map);
    AvroSchemaEncoder encoder = create(store, record);
    GenericRecord avro = encoder.encode(record);

    AmazonDynamoDBAsyncClient client = tables.getAsyncClient();
    DynamoTableTimeManager manager = new DynamoTableTimeManager(client, "test-data");
    DynamoTableCreator writeTableCreator =
      new DynamoTableCreator(manager, new DynamoDB(client), 1, 1);
    AvroToDynamoWriter writer = new AvroToDynamoWriter(client, 1, writeTableCreator);
    writer.write(avro);
    MultiWriteFailures<?> failures = writer.flush();
    assertFalse("Found some write failures! " + failures, failures.any());


    // read the data back through calcite
    LocalDynamoTestUtil util = dynamo.getUtil();
    CalciteAssert.that()
                 .with(new TableModelBuilder()
                   .setDynamo(util.getUrl())
                   .setSchemaTable(tables.getTestTableName())
                   .build())
                 .query(format("select * from \"events\" WHERE " +
                        ORG_ID_KEY + " = '%s' AND " +
                        ORG_METRIC_TYPE_KEY + " = '%s'", org, metric))
                 .returnsCount(1);
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
