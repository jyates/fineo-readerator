package org.apache.calcite.avatica.jdbc;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.fineo.read.FineoProperties;
import io.fineo.read.drill.FineoSqlRewriter;
import io.fineo.read.serve.util.IteratorResult;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.remote.TypedValue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.of;

/**
 * An implementation of JDBC metadata that prevents readers from viewing data that is not present
 * in the specified connection properties
 */
public class FineoJdbcMeta extends JdbcMeta {

  public static final String ORG_PROPERTY_KEY = FineoProperties.COMPANY_KEY_PROPERTY;
  private static final String FINEO_CATALOG = "FINEO_CAT";
  private static final String FINEO_SCHEMA = "FINEO_SCHEMA";
  private static final String FINEO_SCHEMA_TABLE = "FINEO_SCHEMA_TABLE";
  private final FineoSqlRewriter rewrite;
  private final String org;

  private final Cache<String, Map<String, String>> connectionPropertyCache;

  public FineoJdbcMeta(String url, MetricsSystem metrics, String org) throws SQLException {
    this(url, new Properties(), metrics, org);
  }

  public FineoJdbcMeta(String url, Properties info, MetricsSystem metrics, String org) throws
    SQLException {
    super(url, info, metrics);
    this.rewrite = new FineoSqlRewriter(org);
    this.org = org;

    int concurrencyLevel = Integer.parseInt(
      info.getProperty(ConnectionCacheSettings.CONCURRENCY_LEVEL.key(),
        ConnectionCacheSettings.CONCURRENCY_LEVEL.defaultValue()));
    int initialCapacity = Integer.parseInt(
      info.getProperty(ConnectionCacheSettings.INITIAL_CAPACITY.key(),
        ConnectionCacheSettings.INITIAL_CAPACITY.defaultValue()));
    long maxCapacity = Long.parseLong(
      info.getProperty(ConnectionCacheSettings.MAX_CAPACITY.key(),
        ConnectionCacheSettings.MAX_CAPACITY.defaultValue()));
    long connectionExpiryDuration = Long.parseLong(
      info.getProperty(ConnectionCacheSettings.EXPIRY_DURATION.key(),
        ConnectionCacheSettings.EXPIRY_DURATION.defaultValue()));
    TimeUnit connectionExpiryUnit = TimeUnit.valueOf(
      info.getProperty(ConnectionCacheSettings.EXPIRY_UNIT.key(),
        ConnectionCacheSettings.EXPIRY_UNIT.defaultValue()));
    this.connectionPropertyCache = CacheBuilder.newBuilder()
                                               .concurrencyLevel(concurrencyLevel)
                                               .initialCapacity(initialCapacity)
                                               .maximumSize(maxCapacity)
                                               .expireAfterAccess(connectionExpiryDuration,
                                                 connectionExpiryUnit)
                                               .build();
  }


  @Override
  public void openConnection(ConnectionHandle ch, Map<String, String> info) {
    super.openConnection(ch, info);
    this.connectionPropertyCache.put(ch.id, info);
  }

  private String getOrg(ConnectionHandle ch) {
    return getOrg(ch.id);
  }

  private String getOrg(String cid) {
    Map<String, String> props = checkNotNull(connectionPropertyCache.getIfPresent(cid),
      "No connection key found for connection handle '{}'", cid);

    return props.get(ORG_PROPERTY_KEY);
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

  //
  // Write Operations
  //

  @Override
  public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
    String org = getOrg(ch);
    return super.prepare(ch, rewrite(sql, org), maxRowCount);
  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount,
    PrepareCallback callback) throws NoSuchStatementException {
    String org = getOrg(h.connectionId);
    return super.prepareAndExecute(h, rewrite(sql, org), maxRowCount, callback);
  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount,
    int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
    String org = getOrg(h.connectionId);
    return super.prepareAndExecute(h, rewrite(sql, org), maxRowCount, maxRowsInFirstFrame,
      callback);
  }

  private String rewrite(String sql, String org) {
    Preconditions.checkArgument(this.org.equals(org),
      "Routing error! Customer got routed to the wrong JDBC server.");
    try {
      return rewrite.rewrite(sql);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Pat adjust(String org, Pat pattern) {
    return Pat.of(adjust(org, pattern.s));
  }

  private String adjust(String org, String pattern) {
    return org + "." + pattern;
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

  //
  // Unsupported operations
  //

  @Override
  public ExecuteBatchResult executeBatch(StatementHandle h,
    List<List<TypedValue>> updateBatches) throws NoSuchStatementException {
    throwWriteOp();
    return null;
  }

  private void throwWriteOp() {
    throw new UnsupportedOperationException(
      "This adapter can only be used for READ operations, not write operations. See API at "
      + "http://api.fineo.io for how to write data.");
  }
}
