package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

/**
 *
 */
public class TestUserTableVisibility {

  @ClassRule
  public static AwsDynamoResource dynamo =
    new AwsDynamoResource(BaseDynamoTableTest.STATIC_CREDENTIALS_PROVIDER);
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);
  protected static final String PK = "pk";

  @Test
  public void testNoCredentialsLimitsVisibility() throws Exception {
    DynamoStoragePluginConfig conf = Mockito.mock(DynamoStoragePluginConfig.class);
    DynamoStoragePlugin plugin = Mockito.mock(DynamoStoragePlugin.class);
    DynamoSchemaFactory factory = new DynamoSchemaFactory("dynamo", conf, plugin);

    // get the dynamo schema
    SchemaConfig sc = Mockito.mock(SchemaConfig.class);
    SchemaPlus schema = Mockito.mock(SchemaPlus.class);
    AtomicReference<AbstractSchema> ds = new AtomicReference<>();
    Mockito.doAnswer(invocation -> {
      ds.set((AbstractSchema) invocation.getArguments()[1]);
      return null;
    }).when(schema).add(Mockito.eq("dynamo"), Mockito.any(AbstractSchema.class));
    factory.registerSchemas(sc, schema);
    assertNotNull(ds.get());

    // make sure that we can't read any tables in it, even after we create a table
    Table hash = createHashTable();

    // a bit more mocking to ensure we use a non-credential client
    AmazonDynamoDBAsyncClient client = Mockito.mock(AmazonDynamoDBAsyncClient.class);
    when(client.listTables(Mockito.any(ListTablesRequest.class))).thenAnswer(invocation -> {
      ListTablesResult tables = new ListTablesResult();
      tables.withTableNames(hash.getTableName());
      return tables;
    });
    AmazonDynamoDBException exception = new
      AmazonDynamoDBException(
      "User: arn:aws:sts::1234:role/rolename is not authorized to perform: dynamodb:DescribeTable"
      + " on resource: arn:aws:dynamodb:us-east-1:1234:table/" + hash.getTableName());
    exception.setErrorCode("AccessDeniedException");
    exception.setStatusCode(400);
    when(client.describeTable(Mockito.any(DescribeTableRequest.class))).thenThrow(exception);
    DynamoDB dynamo = new DynamoDB(client);
    when(plugin.getModel()).thenReturn(dynamo);

    assertEquals(newHashSet(), ds.get().getTableNames());
  }

  protected Table createHashTable() throws InterruptedException {
    // single hash PK
    ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(new AttributeDefinition()
      .withAttributeName(PK).withAttributeType("S"));
    ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(new KeySchemaElement().withAttributeName(PK).withKeyType(KeyType.HASH));

    CreateTableRequest request = new CreateTableRequest()
      .withKeySchema(keySchema)
      .withAttributeDefinitions(attributeDefinitions);
    return createTable(request);
  }

  protected Table createTable(CreateTableRequest request) throws InterruptedException {
    DynamoDB dynamoDB = new DynamoDB(tables.getAsyncClient());
    request.withProvisionedThroughput(new ProvisionedThroughput()
      .withReadCapacityUnits(5L)
      .withWriteCapacityUnits(6L));

    if (request.getTableName() == null) {
      String tableName = tables.getTestTableName();
      tableName = tableName.replace('-', '_');
      request.setTableName(tableName);
    }

    Table table = dynamoDB.createTable(request);
    table.waitForActive();
    return table;
  }

}
