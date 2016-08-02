package org.apache.calcite.avatica.jdbc;

import io.fineo.read.serve.util.IteratorResult;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.remote.TypedValue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.of;

/**
 * An implementation of JDBC metadata that prevents readers from viewing data that is not present
 * in the specified connection properties
 */
public class FineoJdbcMeta extends JdbcMeta {

  public static final String ORG_PROPERTY_KEY = "COMPANY_KEY";
  private static final String FINEO_CATALOG = "FINEO_CAT";
  private static final String FINEO_SCHEMA = "FINEO_SCHEMA";
  private static final String FINEO_SCHEMA_TABLE = "FINEO_SCHEMA_TABLE";

  public FineoJdbcMeta(String url, MetricsSystem metrics) throws SQLException {
    super(url, new Properties(), metrics);
  }

  @Override
  public MetaResultSet getCatalogs(ConnectionHandle ch) {
    try {
      // only ever one catalog to return - FINEO
      final ResultSet rs = new IteratorResult(FINEO_CATALOG, FINEO_SCHEMA, FINEO_SCHEMA_TABLE,
        of("CATALOG"), of(new Object[]{FINEO_CATALOG}).iterator());
      int stmtId = registerMetaStatement(rs);
      return JdbcResultSet.create(ch.id, stmtId, rs);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private int registerMetaStatement(ResultSet rs) throws SQLException {
    final int id = getStatementIdGenerator().getAndIncrement();
    StatementInfo statementInfo = new StatementInfo(rs.getStatement());
    statementInfo.setResultSet(rs);
    getStatementCache().put(id, statementInfo);
    return id;
  }

  @Override
  public MetaResultSet getSchemas(ConnectionHandle ch, String catalog,
    Pat schemaPattern) {
    String org = getOrg(ch);
    return super.getSchemas(ch, getCatalog(catalog), getSchema(schemaPattern));
  }

  @Override
  public MetaResultSet getTables(ConnectionHandle ch, String catalog,
    Pat schemaPattern, Pat tableNamePattern, List<String> typeList) {
    String org = getOrg(ch);
    return super.getTables(ch, getCatalog(catalog),
      schemaPattern,
      adjust(org, tableNamePattern),
      typeList);
  }

  @Override
  public MetaResultSet getColumns(ConnectionHandle ch, String catalog,
    Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
    String org = getOrg(ch);
    return super.getColumns(ch, getCatalog(catalog),
      getSchema(schemaPattern),
      adjust(org, tableNamePattern),
      columnNamePattern);
  }

  @Override
  public MetaResultSet getProcedures(ConnectionHandle ch, String catalog,
    Pat schemaPattern, Pat procedureNamePattern) {
    String org = getOrg(ch);
    return super.getProcedures(ch, getCatalog(catalog), getSchema(schemaPattern),
      adjust(org, procedureNamePattern));
  }

  @Override
  public MetaResultSet getProcedureColumns(ConnectionHandle ch, String catalog,
    Pat schemaPattern, Pat procedureNamePattern, Pat columnNamePattern) {
    String org = getOrg(ch);
    return super
      .getProcedureColumns(ch, getCatalog(catalog), getSchema(schemaPattern), adjust(org,
        procedureNamePattern),
        columnNamePattern);
  }

  @Override
  public MetaResultSet getTablePrivileges(ConnectionHandle ch, String catalog,
    Pat schemaPattern, Pat tableNamePattern) {
    String org = getOrg(ch);
    return super.getTablePrivileges(ch, getCatalog(catalog), getSchema(schemaPattern), adjust(org,
      tableNamePattern));
  }

  @Override
  public MetaResultSet getColumnPrivileges(ConnectionHandle ch, String catalog,
    String schema, String table, Pat columnNamePattern) {
    String org = getOrg(ch);
    return super
      .getColumnPrivileges(ch, getCatalog(catalog), getSchema(schema), adjust(org, table),
        columnNamePattern);
  }

  /**
   * Write Operations
   */

  @Override
  public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
    String org = getOrg(ch);
    return super.prepare(ch, reparse(sql, org), maxRowCount);
  }

  private String reparse(String sql, String org) {
    return null;
  }

  @Override
  public ExecuteBatchResult executeBatch(StatementHandle h,
    List<List<TypedValue>> updateBatches) throws NoSuchStatementException {
    throwWriteOp();
    return null;
  }

  private void throwWriteOp(){
    throw new UnsupportedOperationException(
      "This adapter can only be used for READ operations, not write operations. See API at "
      + "http://api.fineo.io for how to write data.");
  }

  private Pat adjust(String org, Pat pattern) {
    return Pat.of(adjust(org, pattern.s));
  }

  private String adjust(String org, String pattern) {
    return org + "." + pattern;
  }

  private String getOrg(ConnectionHandle ch) {
    try {
      Properties props = getConnection(ch.id).getClientInfo();
      return props.getProperty(ORG_PROPERTY_KEY);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private final String getCatalog(String specified) {
    if (specified.equals(FINEO_CATALOG)) {
      return "DRILL";
    }
    return "_NOT_A_CATALOG_FINEO_";
  }

  private final Pat getSchema(Pat specified) {
    Pattern pat = Pattern.compile(specified.s);
    if (pat.matcher(FINEO_SCHEMA).matches()) {
      return Pat.of("");
    }
    return Pat.of("_NOT_A_VALID_SCHEMA_FINEO_");
  }

  private final String getSchema(String specified) {
    if (specified.equals(FINEO_SCHEMA)) {
      return "";
    }
    return "_NOT_A_VALID_SCHEMA_FINEO_";
  }
}
