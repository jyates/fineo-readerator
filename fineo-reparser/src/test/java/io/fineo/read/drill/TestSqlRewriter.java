package io.fineo.read.drill;

import org.junit.Test;

import static java.lang.String.format;
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
    String expected = expectSelect("`f1`", getFullTableName("my_table"));
    assertEquals(expected, rewriter.rewrite(sql));
  }

  @Test
  public void testReRewrite() throws Exception {
    String sql = "SELECT * FROM my_table";
    String expected = expectSelectStar("my_table");
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

  @Test
  public void testSelectError() throws Exception {
    String sql = "SELECT * from errors.stream";
    String expected = "SELECT *\nFROM `fineo`.`errors`.`stream`\nWHERE `apikey` = '"+org+"'";
    assertEquals(expected, rewriter.rewrite(sql));
  }

  @Test
  public void testSelectErrorWithWhere() throws Exception {
    String sql = "SELECT * from errors.stream where a='b'";
    String base = "SELECT *\n"+
                  "FROM `fineo`.`errors`.`stream`\n";
    String expected = base + "WHERE `a` = 'b' AND `apikey` = '"+org+"'";
    assertEquals(expected, rewriter.rewrite(sql));
    sql += " and c=1234";
    expected = base + "WHERE `a` = 'b' AND `c` = 1234 AND `apikey` = '"+org+"'";
    assertEquals(expected, rewriter.rewrite(sql));
  }

  @Test
  public void testReadFromFineo() throws Exception {
    String sql = "SELECT * from fineo.sometable";
    assertEquals(expectSelectStar("sometable"), rewriter.rewrite(sql));
  }

  @Test
  public void testReadFromUserErrorTable() throws Exception {
    assertEquals(expectSelectStar("error"), rewriter.rewrite("SELECT * from fineo.error"));
  }

  @Test
  public void testFineoErrorsStream() throws Exception {
    assertEquals("SELECT *\nFROM `fineo`.`"+org+"`.`error`.`stream`",
      rewriter.rewrite("SELECT * from fineo.error.stream"));
  }
  
  private String expectSelectStar(String simpleTable){
    return expectSelect("*", getFullTableName(simpleTable));
  }

  private String expectSelect(String columns, String from){
    return  format("SELECT %s\nFROM %s", columns, from);
  }

  private String getFullTableName(String table){
    return format("`fineo`.`%s`.`%s`", org, table);
  }
}
