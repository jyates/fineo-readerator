package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.junit.Test;

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
    item.with(COL1, "v2");
    Table table = createTableWithItems(item, i2);
    verify(runAndReadResults("SELECT *" + from(table) +
                             "t WHERE t." + PK + " = '2'"),
      i2);
  }
}
