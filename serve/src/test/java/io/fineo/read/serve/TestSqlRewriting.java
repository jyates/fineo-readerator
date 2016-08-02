package io.fineo.read.serve;

import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestSqlRewriting {
  @Test
  public void testSimpleFrom() throws Exception {
    String sql = "Select * from t1";
    String org = "org1";
    assertEquals(format("Select * from %s.t1", org), new FineoSqlRewriter().rewrite(sql, org));
  }
}
