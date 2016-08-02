package io.fineo.read.serve;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class FineoSqlRewriter {

  public String rewrite(String sql, String org) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql);
    SqlNode node = parser.parseStmt();
    if (node instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) node;
      SqlIdentifier from = (SqlIdentifier) select.getFrom();
      List<String> name = newArrayList("fineo", org);
      name.addAll(from.names);
      select.setFrom(new SqlIdentifier(name, from.getCollation(), from.getParserPosition(), null));
    } else {
      throw new UnsupportedOperationException(
        "Cannot handle queries of type: " + node.getClass() + ". Query: " + sql);
    }
    return node.toString();
  }
}
