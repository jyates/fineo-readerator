package io.fineo.read.drill;

import io.fineo.drill.rule.DrillClusterRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestFineoReadTable {

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @Test
  public void test() throws Exception {
    try (Connection conn = drill.getConnection()) {
      setSession(conn, "`exec.errors.verbose` = true");
      setSession(conn, "`store.format`='parquet'");

      String from = "FROM fineo.events";
      ResultSet count = conn.createStatement().executeQuery("SELECT count(*) " + from);
      assertTrue(count.next());
      assertEquals(1, count.getInt(1));
    }
  }

  private void setSession(Connection conn, String stmt) throws SQLException {
    conn.createStatement().execute("ALTER SESSION SET " + stmt);
  }
}
