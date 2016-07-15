package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestDynamoFilterPushDown extends BaseDynamoTest {

  /**
   * Table with just a hash key has the hash key fully specified, which should cause a single Get
   * request
   */
  @Test
  public void testPrimaryKeyFilterSpecifiedHashKey() throws Exception {
    Item item = item();
    item.with(COL1, "v1");
    Item i2 = new Item();
    i2.with(PK, "2");
    i2.with(COL1, "pk");
    Table table = createTableWithItems(item, i2);

    verify(runAndReadResults(selectStarWithPK("2", "t", table)),
      i2);
  }

  @Test
  public void testPrimaryAndSortKeySpecification() throws Exception {
    String pk = "pk", sort = "sort";
    Table table = createHashAndSortTable(pk, sort);
    Item item = new Item();
    item.with(pk, "p1");
    item.with(sort, "s1");
    item.with(COL1, "1");
    table.putItem(item);

    Item item2 = new Item();
    item2.with(pk, "p1");
    item2.with(sort, "s0");
    item2.with(COL1, "2");
    table.putItem(item2);
    // should create a get
//    verify(runAndReadResults(selectStarWithPK("p1", "t", table) + " AND sort = 's1'"), item);
    // should create a query
    verify(runAndReadResults(selectStarWithPK("p1", "t", table) + " AND sort >= 's1'"), item);
  }

  @Test
  public void testPrimaryKeyAndAttributeFilter() throws Exception {
    Item item = item();
    item.with(COL1, "1");
    Table t = createTableWithItems(item);
    verify(runAndReadResults(selectStarWithPK("pk", "t", t) + " AND t." + COL1 + " = '1'"),
      item);

    // number column
    item = new Item();
    item.with(PK, "pk2");
    item.with(COL1, 1);
    t.putItem(item);
    Map<String, Object> row = justOneRow(runAndReadResults(
      selectStarWithPK("pk2", "t", t) + " AND t." + COL1 + " = 1"
    ));
    equalsText(item, PK, row);
    equalsNumber(item, COL1, row);
  }

  @Test
  public void testWhereColumnEqualsNull() throws Exception {
    Item item = item();
    item.with(COL1, null);
    Table table = createTableWithItems(item);

//    QuerySpec spec = new QuerySpec();
//    spec.withKeyConditionExpression("#n0 = :v1");
//    spec.withConsistentRead(true);
//    spec.withFilterExpression("#n1 = :v2");
//    Map<String, Object> valueMap = new HashMap<>();
//    valueMap.put(":v1", "pk");
//    valueMap.put(":v2", null);
//    spec.withValueMap(valueMap);
//    Map<String, String> nameMap = new HashMap<>();
//    nameMap.put("#n0", PK);
//    nameMap.put("#n1", COL1);
//    spec.withNameMap(nameMap);
//    IteratorSupport<Item, QueryOutcome> iter = table.query(spec).iterator();
//    while (iter.hasNext()) {
//      Item i = iter.next();
//      System.out.println(i);
//    }

    verify(runAndReadResults(
      selectStarWithPK("pk", "t", table) + " AND t." + COL1 + " = cast(null as varchar)"),
      item);
  }

  /**
   * Similar to above, but we check for the non-existance of a column
   */
  @Test
  public void testWhereColumnIsNull() throws Exception {
    Item item = item();
    Table table = createTableWithItems(item);
    assertEquals("Should not have found a row when checking for = null and column not set!",
      0,
      runAndReadResults(
        selectStarWithPK("pk", "t", table) + " AND t." + COL1 + " = cast(null as varchar)").size());
    verify(runAndReadResults(selectStarWithPK("pk", "t", table) + " AND t." + COL1 + " IS NULL"),
      item);
  }

  @Test
  public void testSimpleScan() throws Exception {
    Item item = item();
    item.with(COL1, 1);
    Table table = createTableWithItems(item);
    Map<String, Object> row = justOneRow(runAndReadResults("SELECt *" + from(table) + "t WHERE t"
                                                           + "." + COL1 + " = 1"));
    equalsText(item, PK, row);
    equalsNumber(item, COL1, row);
  }


  private String selectStarWithPK(String pk, String tableName, Table table) {
    return "SELECT *" + from(
      table) + tableName + " WHERE " + tableName + "." + PK + " = '" + pk + "'";
  }
}
