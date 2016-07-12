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
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.util.Text;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
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
    item.withBoolean("col5_boolean", true);

    table.putItem(item);
    selectStar(table, item);
  }

  /**
   * All numbers are read as strings. Ensure that they preserve the representation
   *
   * @throws Exception on failure
   */
  @Test
  public void testBigDecimalAsString() throws Exception {
    Table table = createHashTable();
    Item item = item();
    String d1 = "4111111111111111111111111111111111111";
    String c1 = "col4_big_Decimal_noScale";
    item.with(c1, new BigDecimal(d1));
    String d2 = "4.222222222222222222222222222222222222";
    String c2 = "col4_big_Decimal";
    item.with(c2, new BigDecimal(d2));
    table.putItem(item);

    List<Map<String, Object>> values = selectStar(table, false, item);
    assertEquals("Got more than 1 row! Values: " + values, 1, values.size());
    Map<String, Object> row = values.get(0);
    assertEquals(item.get(c1), new BigDecimal(row.get(c1).toString()));
    assertEquals(item.get(c2), new BigDecimal(row.get(c2).toString()));
  }

  @Test
  public void testCastValues() throws Exception {
    Item item = item();
    item.withFloat("c1", 1.1f);
    item.withInt("c2", 2);
    item.withDouble("c3", 3.3);
    item.withLong("c4", 5l);
    Table table = createTableWithItems(item);

    List<Map<String, Object>> rows =
      runAndReadResults("SELECT " +
                        "CAST(c1 as float) as c1, " +
                        "CAST(c2 as int) as c2, " +
                        "CAST(c3 as double) as c3, " +
                        "CAST(c4 as bigint) as c4 " +
                        from(table));

    assertEquals("Got rows: " + rows, 1, rows.size());
    Map<String, Object> expected = new HashMap<>();
    expected.put("c1", 1.1f);
    expected.put("c2", 2);
    expected.put("c3", 3.3);
    expected.put("c4", 5l);
    assertEquals(expected, rows.get(0));
  }


  /**
   * Just request List columns (map, list), rather than trying to read a value out of
   * the column (that's another test).
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

  /**
   * VarChar list elements behave (for some reason) differently than booleans when setting the
   * values. This makes us set the list value count in the list method, rather than at the end of
   * #next() when we set all the other top level lengths
   */
  @Test
  public void testVarCharList() throws Exception {
    Item item = new Item();
    item.with(PK, "pk_val_1");
    List<String> values = newArrayList("a");
    item.withList(COL1, values);
    Table t = createTableWithItems(item);
    List<Map<String, Object>> rows = selectStar(t, false, item);
    List<Text> expected = values.stream().map(s -> new Text(s)).collect(Collectors.toList());
    assertEquals("Only expected one row, got: " + rows, 1, rows.size());
    Map<String, Object> row = rows.get(0);
    assertEquals("Only expected two fields! Got: " + row, 2, row.size());
    assertEquals("Wrong values for list/set column!", expected, row.get(COL1));

  }

  @Test
  public void testListReadIntoList() throws Exception {
    Item item = new Item();
    item.with(PK, "pk_val_1");
    List<Boolean> list = newArrayList(true, true, false, true);
    item.withList(COL1, list);
    Table table = createTableWithItems(item);

    String sql = "SELECT " + COL1 + "[0] as c1," + COL1 + "[2] as c2" + from(table);
    Map<String, Object> row = justOneRow(runAndReadResults(sql));
    assertEquals("Expected two columns, one for each list element. Got: " + row, 2, row.size());
    assertEquals("Mismatch for first column!", list.get(0), row.get("c1"));
    assertEquals("Mismatch for second column!", list.get(2), row.get("c2"));
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
    Map<String, Object> row = justOneRow(selectStar(t, false, item));
    List<Text> expected = values.stream().map(s -> new Text(s)).collect(Collectors.toList());
    assertEquals("Only expected two fields! Got: " + row, 2, row.size());
    assertEquals("Wrong values for list/set column!", expected, row.get(COL1));
  }

  private Item item() {
    Item item = new Item();
    item.with(PK, "pk");
    return item;
  }

  @Test
  public void testReadMap() throws Exception {
    Table table = createHashTable();
    Item item = item();
    Map<String, Boolean> c1 = new HashMap<>();
    c1.put("c1.1", true);
    c1.put("c1.2", false);
    String boolMapKey = "col6_map_bool";
    item.withMap(boolMapKey, c1);
    Map<String, String> c2 = new HashMap<>();
    c2.put("c2.1", "v1");
    c2.put("c2.2", "v2");
    String stringMapKey = "col6-2_map_string";
    item.withMap(stringMapKey, c2);

    table.putItem(item);
    Map<String, Object> row = justOneRow(selectStar(table, false, item));
    Map<String, Object> expected = new HashMap<>();
    expected.put(PK, new Text("pk"));

    Map<String, Boolean> expectedBoolMap = new JsonStringHashMap<>();
    expectedBoolMap.putAll(c1);
    expected.put(boolMapKey, expectedBoolMap);

    Map<String, Text> expectedStringMap = new JsonStringHashMap<>();
    c2.entrySet().stream().forEach(e -> expectedStringMap.put(e.getKey(), new Text(e.getValue())));
    expected.put(stringMapKey, expectedStringMap);

    assertEquals(expected, row);
  }

  @Test
  public void testReadIntoMap() throws Exception {
    Item item = item();
    Map<String, Boolean> c1 = new HashMap<>();
    c1.put("c1_1", true);
    c1.put("c1_2", false);
    item.with(COL1, c1);
    Table table = createTableWithItems(item);

    Map<String, Object> row =
      // maps require specifying the table when reading them
      justOneRow(runAndReadResults("SELECT t." + COL1 + ".c1_1 as c1" + from(table) + " t"));
    assertEquals(true, row.get("c1"));
  }

  @Test
  public void testMapNestedInList() throws Exception {
    Item item = item();
    List<Map<String, Boolean>> list = new ArrayList<>();
    Map<String, Boolean> c1 = new HashMap<>();
    c1.put("c1_1", true);
    c1.put("c1_2", true);
    list.add(c1);

    Map<String, Boolean> c2 = new HashMap<>();
    c2.put("c2_1", false);
    c2.put("c2_2", false);
    list.add(c2);
    item.with(COL1, list);
    Table table = createTableWithItems(item);

    Map<String, Object> nested =
      justOneRow(runAndReadResults("SELECT t." + COL1 + "[0].c1_2 as c1" + from(table) + " t"));
    assertEquals(true, nested.get("c1"));
    nested =
      justOneRow(runAndReadResults("SELECT t." + COL1 + "[1].c2_2 as c1" + from(table) + " t"));
    assertEquals(false, nested.get("c1"));
  }

  @Test
  public void testReadListNestedInMap() throws Exception {
    Item item = item();
    List<String> l1 = newArrayList("l1_1_value", "l1_2");
    List<String> l2 = newArrayList("l2_1_value", "l2_2");
    Map<String, Object> c1 = new HashMap<>();
    c1.put("l1", l1);
    c1.put("l2", l2);
    item.with(COL1, c1);
    Table table = createTableWithItems(item);
    Map<String, Object> result = justOneRow(runAndReadResults("SELECT t." + COL1 + ".l1[1] as "
                                                              + "c1" + from(table) + " t"));
    assertEquals(new Text(l1.get(1)), result.get("c1"));
  }

  private Map<String, Object> justOneRow(List<Map<String, Object>> rows) {
    assertEquals("Got more rows than expected! Rows: " + rows, 1, rows.size());
    return rows.get(0);
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
    String sql = "SELECT *" + from(table);
    List<Map<String, Object>> rows = runAndReadResults(sql);
    if (verify) {
      verify(rows, items);
    }
    return rows;
  }

  private String from(Table table) {
    return " FROM dynamo." + table.getTableName();
  }

  private List<Map<String, Object>> runAndReadResults(String sql) throws Exception {
    List<QueryDataBatch> results = testSqlWithResults(sql);
    return readObjects(results);
  }

  private void verify(List<Map<String, Object>> rows, Item... items) {
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
