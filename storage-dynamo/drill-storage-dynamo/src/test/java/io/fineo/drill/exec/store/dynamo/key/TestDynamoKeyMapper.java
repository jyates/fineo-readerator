package io.fineo.drill.exec.store.dynamo.key;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import io.fineo.drill.exec.store.dynamo.BaseDynamoTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.ImmutableList.of;

public class TestDynamoKeyMapper extends BaseDynamoTest {

  @BeforeClass
  public static void setupCluster() throws Exception {
    BaseDynamoTest.setupDefaultTestCluster();
  }

  @Test
  public void testHashKeyMapping() throws Exception {
    Item item = new Item();
    item.with(PK, "ab");
    Table table = createTableWithItems(item);
    updatePlugin(plugin -> {
      Map<String, Object> args = new HashMap<>();
      args.put("@class", LengthBasedTwoPartHashKeyMapper.class.getName());
      args.put("length", 1);
      DynamoKeyMapperSpec spec = new DynamoKeyMapperSpec(of("h1", "h2"), of("S", "S"), args);
      plugin.setDynamoKeyMapperForTesting(table.getTableName(), spec);
    });

    Item expected = new Item();
    expected.with("h1", "a");
    expected.with("h2", "b");
    selectStar(table, true, expected);
  }

  @Test
  public void testTestHashAndSortKeyMapping() throws Exception {
    String sort = "sort";
    Item item = new Item();
    item.with(PK, "ab");
    item.with(sort, "12");
    Table table = createHashAndSortTable(PK, sort);
    table.putItem(item);

    updatePlugin(plugin -> {
      Map<String, Object> args = new HashMap<>();
      args.put("@class", LengthBasedCompoundKeyMapper.class.getName());
      args.put("length", 1);
      DynamoKeyMapperSpec spec = new DynamoKeyMapperSpec(of("h1", "h2", "h3", "h4"), of("S", "S",
        "N", "N"),
        args);
      plugin.setDynamoKeyMapperForTesting(table.getTableName(), spec);
    });

    Item expected = new Item();
    expected.with("h1", "a");
    expected.with("h2", "b");
    expected.with("h3", 1);
    expected.with("h4", 2);
    selectStar(table, true, expected);
  }
}
