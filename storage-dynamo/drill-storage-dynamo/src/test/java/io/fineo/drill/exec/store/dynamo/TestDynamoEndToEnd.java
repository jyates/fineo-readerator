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
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.Text;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.NotSupportedException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
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
    // write a single row to the test table
    Item write = item();
    String col1Value = "2_col1_value";
    write.withString(COL1, col1Value);

    // read that row back with drill
    putAndSelectStar(write);
  }

  @Test
  public void testMultipleRowsStringReadWrite() throws Exception {
    Item item1 = new Item();
    item1.with(PK, "pk1");
    item1.with(COL1, "c1v1");

    Item item2 = new Item();
    item2.with(PK, "pk2");
    item2.with(COL1, "c1v2");
    putAndSelectStar(item1, item2);
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
    Item item = item();
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
   * Just request List columns (map, list), rather than trying to read a value out of
   * the column (that's another test).
   *
   * @throws Exception on failure
   */
  @Test
  public void testListColumnsSimpleRead() throws Exception {
    Item item = new Item();
    item.with(PK, "pk_val_1");
    item.withList("col7-1_list_bool", true, true, false, true);
    Table table = putAndSelectStar(item);

    Item item2 = new Item();
    item2.with(PK, "pk_val_2");
    item2.withList("col7-1_list_bool", false, false, true, false);
    table.putItem(item2);
    selectStar(table, item, item2);
  }

  @Test
  public void testVarCharList() throws Exception {
    Item item = new Item();
    item.with(PK, "pk_val_1");
    item.withList("col7-1_list_bool", "a");
    putAndSelectStar(item);
  }

  @Test
  public void testListReadIntoList() throws Exception {
    Item item = new Item();
    item.with(PK, "pk_val_1");
    List<Boolean> list = newArrayList(true, true, false, true);
    item.withList(COL1, list);
    Table table = createTableWithItems(item);

    String sql = "SELECT " + COL1 + "[0]," + COL1 + "[2] FROM dynamo." + table.getTableName();
    List<Map<String, Object>> rows = runAndReadResults(sql);
    assertEquals(1, rows.size());
    Map<String, Object> row = rows.get(0);
    assertEquals("Expected two columns, one for each list element. Got: " + row, 2, row.size());
    assertEquals("Mismatch for first column!", list.get(0), row.get("EXPR$0"));
    assertEquals("Mismatch for second column!", list.get(2), row.get("EXPR$1"));
  }

  @Test
  public void testSetsAsLists() throws Exception {
    Item item = item();
    List<String> values = newArrayList("a", "b", "c");
    Set<String> set = new HashSet<>();
    set.addAll(values);
    item.withStringSet(COL1, set);
    Table t = createHashTable();
    t.putItem(item);

    item.removeAttribute(COL1);
    item.withList(COL1, values);
    // strings are returned as Text values, so we need to convert the expectation here
    List<Map<String, Object>> rows = selectStar(t, false, item);
    List<Text> expected = values.stream().map(s -> new Text(s)).collect(Collectors.toList());
    assertEquals("Only expected one row, got: " + rows, 1, rows.size());
    Map<String, Object> row = rows.get(0);
    assertEquals("Only expected two fields! Got: " + row, 2, row.size());
    assertEquals("Wrong values for list/set column!", expected, row.get(COL1));
  }

  private Item item() {
    Item item = new Item();
    item.with(PK, "pk");
    return item;
  }

  @Test
  public void testtestMapColumnsSimpleRead() throws Exception {
    Table table = createHashTable();
    Item item = item();
    //    Map<String, Object> c6 = new HashMap<>();
//    c6.put("c6.1.1", "v_c6.1");
//    c6.put("c6.1.2", "v_c6.2");
//    item.withMap("col6_map_string", c6);
//    Map<String, Object> c62 = new HashMap<>();
//    c62.put("c6.2.1", true);
//    c62.put("c6.2.2", false);
//    item.withMap("col6-2_map_bool", c62);

//    item.withList("col7_list_string", "v7.1", "v7.2", "v7.3");
    item.withList("col7-1_list_bool", true, true);//, false, true);

//    item.withStringSet("col8_set_string", "a", "b", "c");
//    item.withBinarySet("col8-1_set_binary", new byte[]{1}, new byte[]{2});
    table.putItem(item);
    selectStar(table, item);
  }

  private Table putAndSelectStar(Item... items) throws Exception {
    Table table = createHashTable();
    for (Item item : items) {
      table.putItem(item);
    }
    selectStar(table, items);
    return table;
  }

  private void selectStar(Table table, Item... items) throws Exception {
    selectStar(table, true, items);
  }

  private List<Map<String, Object>> selectStar(Table table, boolean verify, Item... items) throws
    Exception {
    String sql = "SELECT * FROM dynamo." + table.getTableName();
    List<Map<String, Object>> rows = runAndReadResults(sql);
    if (verify) {
      verify(rows, items);
    }
    return rows;
  }

  private List<Map<String, Object>> runAndReadResults(String sql) throws Exception {
    List<QueryDataBatch> results = testSqlWithResults(sql);
    return readObjects(results);
  }

  private void verify(List<Map<String, Object>> rows, Item[] items) {
    assertEquals("Wrong number of expected rows! Got rows: " + rows + "\nExpected: " + items,
      rows.size(), items.length);
    for (int i = 0; i < items.length; i++) {
      Map<String, Object> row = rows.get(i);
      Item item = items[i];
      assertEquals("Wrong number of fields in row! Got row: " + row + "\nExpected: " + item,
        row.size(), item.asMap().size());
      for (Map.Entry<String, Object> field : row.entrySet()) {
        String name = field.getKey();
        Object o = field.getValue();
        if (o instanceof Text) {
          o = o.toString();
        }
        if (o instanceof byte[]) {
          assertArrayEquals("Array mismatch for: " + name, (byte[]) item.get(name), (byte[]) o);
        } else {
          assertEquals("Mismatch for: " + name, item.get(name), o);
        }
      }
    }
  }

  private List<Map<String, Object>> readObjects(List<QueryDataBatch> results) throws
    SchemaChangeException {
    List<Map<String, Object>> rows = new ArrayList<>();
    final RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for (final QueryDataBatch result : results) {
      loader.load(result.getHeader().getDef(), result.getData());
      List<Map<String, Object>> read = readRow(loader);
      rows.addAll(read);
      loader.clear();
      result.release();
    }
    return rows;
  }


  private List<Map<String, Object>> readRow(RecordBatchLoader loader) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int row = 0; row < loader.getRecordCount(); row++) {
      Map<String, Object> rowMap = new HashMap<>();
      rows.add(rowMap);
      for (VectorWrapper<?> vw : loader) {
        MaterializedField field = vw.getField();
        String name = field.getName();
        Object o = vw.getValueVector().getAccessor().getObject(row);
        rowMap.put(name, o);
      }
    }

    for (VectorWrapper<?> vw : loader) {
      vw.clear();
    }
    return rows;
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

  private Table createTableWithItems(Item... items) throws InterruptedException {
    Table table = createHashTable();
    for (Item item : items) {
      table.putItem(item);
    }
    return table;
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
