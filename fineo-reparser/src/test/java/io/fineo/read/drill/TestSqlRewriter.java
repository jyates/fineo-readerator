package io.fineo.read.drill;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 *
 */
public class TestSqlRewriter {

  private final String org = "org1";
  private final FineoSqlRewriter rewriter = new FineoSqlRewriter(org);

  @Test
  public void testFieldNotRewritten() throws Exception {
    String sql = "SELECT f1 FROM my_table";
    String expected = "SELECT `f1`\nFROM `fineo`.`" + org + "`.`my_table`";
    assertEquals(expected, rewriter.rewrite(sql));
  }

  @Test
  public void testReRewrite() throws Exception {
    String sql = "SELECT * FROM my_table";
    String expected = "SELECT *\nFROM `fineo`.`" + org + "`.`my_table`";
    assertEquals("Rewrite wrote incorrectly!", expected, rewriter.rewrite(sql));
    assertEquals("Rewrite a rewrite changed the output!", expected,
      rewriter.rewrite(rewriter.rewrite(sql)));
  }

  @Test
  public void testNoRewriteShow() throws Exception {
    String sql = "SHOW tables in Fineo";
    assertEquals("SHOW TABLES IN `Fineo`", rewriter.rewrite(sql));
  }

  /**
   * Regression test to ensure that we can parse FROM(VALUES...) clauses
   * @throws Exception
   */
  @Test
  public void testSelectValues() throws Exception {
    String expected = "SELECT *\nFROM (VALUES ROW(1))";
    assertEquals(expected, rewriter.rewrite("select * from (VALUES 1)"));
    assertEquals(expected, rewriter.rewrite("select * from (VALUES(1))"));
  }
}
