package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import io.fineo.drill.exec.store.dynamo.config.StaticCredentialsConfig;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.NotSupportedException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestDynamoEndToEnd extends BaseTestQuery {

  @ClassRule
  public static AwsDynamoResource dynamo =
    new AwsDynamoResource(BaseDynamoTableTest.STATIC_CREDENTIALS_PROVIDER);
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);

  private static DynamoStoragePlugin storagePlugin;
  private static DynamoStoragePluginConfig storagePluginConfig;

  private static final String PK = "pk";
  private static final String COL1 = "col1";

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    BaseTestQuery.setupDefaultTestCluster();

    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    storagePlugin = (DynamoStoragePlugin) pluginRegistry.getPlugin(DynamoStoragePlugin.NAME);
    storagePluginConfig = (DynamoStoragePluginConfig) storagePlugin.getConfig();
    storagePluginConfig.setEnabled(true);

    DynamoEndpoint endpoint = new DynamoEndpoint(dynamo.getUtil().getUrl());
    storagePluginConfig.setEndpointForTesting(endpoint);

    Map<String, Object> credentials = new HashMap<>();
    AWSCredentials creds = BaseDynamoTableTest.STATIC_CREDENTIALS_PROVIDER.getCredentials();
    StaticCredentialsConfig credentialsConfig = new StaticCredentialsConfig(creds
      .getAWSAccessKeyId(), creds.getAWSSecretKey());
    credentialsConfig.setCredentials(credentials);
    storagePluginConfig.setCredentialsForTesting(credentials);

    ParallelScanProperties scan = new ParallelScanProperties();
    scan.setMaxSegments(10);
    scan.setLimit(1);
    scan.setSegmentsPerEndpoint(1);
    storagePluginConfig.setScanPropertiesForTesting(scan);

    pluginRegistry.createOrUpdate(DynamoStoragePlugin.NAME, storagePluginConfig, true);
  }

  @Test
  public void testSimpleReadWrite() throws Exception {
    Table table = createHashTable();
    // write a single row to the test table
    Item write = new Item();
    String pkValue = "1_pk_value";
    write.withString(PK, pkValue);
    String col1Value = "2_col1_value";
    write.withString(COL1, col1Value);
    table.putItem(write);

    // read that row back with drill
    List<QueryDataBatch> results =
      testSqlWithResults("SELECT * FROM dynamo." + table.getTableName());
    printResult(results);
    assertEquals(1, results.size());
  }

  @Test
  public void testCompoundKey() throws Exception {
    String pk = "id", sort = "sort_key";
    ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(new AttributeDefinition()
      .withAttributeName(pk).withAttributeType("S"));
    attributeDefinitions.add(new AttributeDefinition().withAttributeName(sort).withAttributeType(
      ScalarAttributeType.S));
    ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(new KeySchemaElement().withAttributeName(pk).withKeyType(KeyType.HASH));
    keySchema.add(new KeySchemaElement().withAttributeName(sort).withKeyType(KeyType.RANGE));

    CreateTableRequest request = new CreateTableRequest()
      .withKeySchema(keySchema)
      .withAttributeDefinitions(attributeDefinitions);
    Table table = createTable(request);

    // write a single row to the test table
    Item write = new Item();
    String pkValue = "1_pk_value";
    write.withString(pk, pkValue);
    String sortValue = "1a_sort_value";
    write.withString(sort, sortValue);
    String col1 = "col1";
    String col1Value = "2_col2_value";
    write.withString(col1, col1Value);
    table.putItem(write);

    // read that row back with drill
    List<QueryDataBatch> results =
      testSqlWithResults("SELECT * FROM dynamo." + table.getTableName());
    printResult(results);
    assertEquals(1, results.size());
  }

  @Test
  public void testScalar() throws Exception {
    Table table = createHashTable();
    Item item = new Item();
    item.withString(PK, "pk_value");
    item.withBinary(COL1, new byte[]{1, 2, 3, 4});
    item.withBinary("col2_bb", ByteBuffer.wrap(new byte[]{5, 6, 7, 8}));
    // numbers are all converted to BigDecimals on storage
    item.withInt("col3_int", 1);
    item.withFloat("col4_float", 4.1f);

    item.withBoolean("col5_boolean", true);

    table.putItem(item);
    selectStar(table, item);
  }

  /**
   * Just request the complex type columns (map, list), rather than trying to read a value out of
   * the column (that's another test).
   * @throws Exception on failure
   */
  @Test
  public void testComplexTypesSimpleRead() throws Exception {
    Table table = createHashTable();
    Item item = new Item();
    item.with(PK, "pk_val");
//    Map<String, Object> c6 = new HashMap<>();
//    c6.put("c6.1.1", "v_c6.1");
//    c6.put("c6.1.2", "v_c6.2");
//    item.withMap("col6_map_string", c6);
//    Map<String, Object> c62 = new HashMap<>();
//    c62.put("c6.2.1", true);
//    c62.put("c6.2.2", false);
//    item.withMap("col6-2_map_bool", c62);

//    item.withList("col7_list_string", "v7.1", "v7.2", "v7.3");
    item.withList("col7-1_list_bool", true, false, true);

//    item.withStringSet("col8_set_string", "a", "b", "c");
//    item.withBinarySet("col8-1_set_binary", new byte[]{1}, new byte[]{2});
    table.putItem(item);
    selectStar(table, item);
  }

  private void selectStar(Table table, Item... items) throws Exception {
    runAndVerify("SELECT * FROM dynamo." + table.getTableName(), items);
  }

  private void runAndVerify(String sql, Item... items) throws Exception {
    List<QueryDataBatch> results = testSqlWithResults(sql);
    int rowCount = 0;

    final RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for (final QueryDataBatch result : results) {
      loader.load(result.getHeader().getDef(), result.getData());
      rowCount += result.getHeader().getRowCount();
      verifyRows(loader, items);
      loader.clear();
      result.release();
    }
    assertEquals(1, rowCount);
  }

  private void verifyRows(RecordBatchLoader loader, Item... items) {
    for (int row = 0; row < loader.getRecordCount(); row++) {
      Item item = items[row];
      for (VectorWrapper<?> vw : loader) {
        MaterializedField field = vw.getField();
        String name = field.getName();
        Object o = vw.getValueVector().getAccessor().getObject(row);
        if (o instanceof byte[]) {
          assertArrayEquals("Array mismatch for: " + name, (byte[]) item.get(name), (byte[]) o);
        } else {
          assertEquals("Mismatch for: " + name, item.get(name), o);
        }
      }
    }

    for (VectorWrapper<?> vw : loader) {
      vw.clear();
    }
  }

  /**
   * Ensure that we can correctly convert from the dynamo number to an "easier" type
   *
   * @throws Exception on failure
   */
  @Test
  public void testConvertNumbers() throws Exception {
    Table table = createHashTable();
    throw new NotSupportedException("Need to implement!");
  }

  private Table createHashTable() throws InterruptedException {
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

  private Table createTable(CreateTableRequest request) throws InterruptedException {
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
