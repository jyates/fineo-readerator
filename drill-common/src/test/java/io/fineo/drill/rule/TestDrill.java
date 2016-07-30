package io.fineo.drill.rule;

import com.fasterxml.jackson.jr.ob.JSON;
import org.apache.drill.jdbc.Driver;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Simple test to ensure that drill rules work as expected
 */
public class TestDrill {

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void setup() {
    Driver.load();
  }

  @Test
  public void testReadWrite() throws Exception {
    // writer a simple json file
    Map<String, Object> json = new HashMap<>();
    json.put("a", "c");

    File tmp = folder.newFolder("drill");
    File out = new File(tmp, "test.json");
    JSON j = JSON.std;
    j.write(json, out);

    try (Connection conn = drill.getConnection()) {
      conn.createStatement().execute("ALTER SESSION SET `store.format`='json'");
      String select = String.format("SELECT * FROM dfs.`%s`", out.getPath());
      ResultSet results = conn.createStatement().executeQuery(select);
      assertTrue(results.next());
      assertEquals(json.get("a"), results.getString("a"));
      assertEquals(1, results.getMetaData().getColumnCount());
    }
  }
}
