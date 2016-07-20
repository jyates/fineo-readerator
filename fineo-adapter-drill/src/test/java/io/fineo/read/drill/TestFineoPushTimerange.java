package io.fineo.read.drill;

import com.google.common.collect.ImmutableList;
import io.fineo.read.drill.exec.store.plugin.SourceFsTable;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Ensure that we push the timerange down into the scan when applicable
 */
public class TestFineoPushTimerange extends BaseFineoTest {
  @Test
  public void testPushTimerangeIntoFileQuery() throws Exception {
    TestState state = register();

    Map<String, Object> values = newHashMap();
    values.put(fieldname, false);
    File tmp = folder.newFolder("drill");
    List<SourceFsTable> files = new ArrayList<>();
    files.add(state.write(tmp, 10001, values));

    // ensure that the fineo-test plugin is enabled
    bootstrap(files.toArray(new SourceFsTable[0]));

    String query = verifySelectStar(ImmutableList.of("`timestamp` > 10000"),
      result -> assertNext(result, values));
    Connection conn = drill.getConnection();
    String explain = explain(query);
    ResultSet plan = conn.createStatement().executeQuery(explain);
    System.out.println(plan);
  }

  private String explain(String sql) {
    return "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITH IMPLEMENTATION FOR " + sql;
  }
}
