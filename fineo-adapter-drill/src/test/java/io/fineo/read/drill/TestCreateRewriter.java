package io.fineo.read.drill;

import org.junit.Test;

/**
 *
 */
public class TestCreateRewriter {

  @Test
  public void test() throws Exception {
    FineoSqlRewriter fineoSqlRewriter = new FineoSqlRewriter("org");
    fineoSqlRewriter.rewrite("SELECT * from table1");
  }
}
