package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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
//    QuerySpec spec = new QuerySpec();
//    spec.withKeyConditionExpression("pk = :v1");
//    spec.withFilterExpression("col1 = :v2");
//    Map<String, Object> valueMap = new HashMap<>();
//    valueMap.put(":v1", "2");
//    valueMap.put(":v2", "pk");
//    spec.withValueMap(valueMap);
//    IteratorSupport<Item, QueryOutcome> iter = table.query(spec).iterator();
//    while (iter.hasNext()) {
//      Item i = iter.next();
//      System.out.println(i);
//    }

    verify(runAndReadResults("SELECT *" + from(table) +
                             "t WHERE t." + PK + " = '2'"),
//                             "t WHERE t." + PK + " = cast(null as varchar)"),
      i2);
  }
}
