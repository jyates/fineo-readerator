package org.apache.calcite.avatica.jdbc;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.jdbc.JdbcResultSet;
import org.apache.calcite.avatica.jdbc.StatementInfo;
import org.apache.calcite.avatica.metrics.MetricsSystemConfiguration;
import org.apache.calcite.avatica.metrics.dropwizard3.DropwizardMetricsSystemFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * An implementation of JDBC metadata that prevents readers from viewing data that is not present
 * in the specified connection properties
 */
public class FineoJdbcMeta extends JdbcMeta {

  public static final String ORG_PROPERTY_KEY = "COMPANY_KEY";

  public FineoJdbcMeta(String url, MetricsSystemConfiguration metrics) throws SQLException {
    super(url, new Properties(), new DropwizardMetricsSystemFactory().create(metrics));
  }

  @Override
  public MetaResultSet getCatalogs(ConnectionHandle ch) {
    try {
      String org = getOrg(ch);
      final ResultSet rs = getConnection(ch.id).getMetaData().getCatalogs();
      // wrapper result set that limits catalog entries to those with the schema prefix


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
    return super.getSchemas(ch, catalog, adjust(org, schemaPattern));
  }

  @Override
  public MetaResultSet getTables(ConnectionHandle ch, String catalog,
    Pat schemaPattern, Pat tableNamePattern, List<String> typeList) {
    String org = getOrg(ch);
    return super.getTables(ch, catalog,
      adjust(org, schemaPattern),
      adjust(org, tableNamePattern),
      typeList);
  }

  @Override
  public MetaResultSet getColumns(ConnectionHandle ch, String catalog,
    Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
    String org = getOrg(ch);
    return super.getColumns(ch, catalog,
      adjust(org, schemaPattern),
      adjust(org, tableNamePattern),
      columnNamePattern);
  }

  @Override
  public MetaResultSet getProcedures(ConnectionHandle ch, String catalog,
    Pat schemaPattern, Pat procedureNamePattern) {
    String org = getOrg(ch);
    return super.getProcedures(ch, catalog, adjust(org, schemaPattern),
      adjust(org, procedureNamePattern));
  }

  @Override
  public MetaResultSet getProcedureColumns(ConnectionHandle ch, String catalog,
    Pat schemaPattern, Pat procedureNamePattern, Pat columnNamePattern) {
    String org = getOrg(ch);
    return super
      .getProcedureColumns(ch, catalog, adjust(org, schemaPattern), adjust(org,
        procedureNamePattern),
        columnNamePattern);
  }

  @Override
  public MetaResultSet getTablePrivileges(ConnectionHandle ch, String catalog,
    Pat schemaPattern, Pat tableNamePattern) {
    String org = getOrg(ch);
    return super.getTablePrivileges(ch, catalog, adjust(org, schemaPattern), adjust(org,
      tableNamePattern));
  }

  @Override
  public MetaResultSet getColumnPrivileges(ConnectionHandle ch, String catalog,
    String schema, String table, Pat columnNamePattern) {
    String org = getOrg(ch);
    return super.getColumnPrivileges(ch, catalog, adjust(org, schema), adjust(org, table),
      columnNamePattern);
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
}
