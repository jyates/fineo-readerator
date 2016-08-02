package io.fineo.read.drill;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.drill.exec.planner.physical.PlannerSettings;

/**
 * Rewrite SQL queries for a single org
 */
public class FineoSqlRewriter {

  private final SqlParser.Config config;

  public FineoSqlRewriter(String org) {
    config = new ParserConfig(org);
  }

  public String rewrite(String sql) throws SqlParseException {
    SqlNode node = parse(sql);
    return node.toString();
  }

  private SqlNode parse(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, config);
    return parser.parseStmt();
  }

  // Copied from Drill SqlConverter.ParserConfig
  private class ParserConfig implements SqlParser.Config {

    final long identifierMaxLength = PlannerSettings.DEFAULT_IDENTIFIER_MAX_LENGTH;

    private final SqlParserImplFactory factory;

    public ParserConfig(String org) {
      this.factory = FineoDrillParserWithCompoundIdConverter.Factory(org);
    }

    @Override
    public int identifierMaxLength() {
      return (int) identifierMaxLength;
    }

    @Override
    public Casing quotedCasing() {
      return Casing.UNCHANGED;
    }

    @Override
    public Casing unquotedCasing() {
      return Casing.UNCHANGED;
    }

    @Override
    public Quoting quoting() {
      return Quoting.BACK_TICK;
    }

    @Override
    public boolean caseSensitive() {
      return false;
    }

    @Override
    public SqlParserImplFactory parserFactory() {
      return factory;
    }

  }
}
