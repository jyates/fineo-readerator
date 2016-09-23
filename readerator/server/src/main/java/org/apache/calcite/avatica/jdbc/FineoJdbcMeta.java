package org.apache.calcite.avatica.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.fineo.read.FineoJdbcProperties;
import io.fineo.read.drill.FineoSqlRewriter;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.remote.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An implementation of JDBC metadata that prevents readers from viewing data that is not present
 * in the specified connection properties.
 * <p>
 * This is used in conjunction with the {@link io.fineo.read.serve.driver.FineoServerDriver} and
 * {@link io.fineo.read.serve.driver.FineoDatabaseMetaData} to prevent access. <tt>this</tt> handles
 * the actual query wrapping and leaves the metadata lookup to the custom driver/metadata
 * </p>
 */
public class FineoJdbcMeta extends JdbcMeta {

  private static final Logger LOG = LoggerFactory.getLogger(FineoJdbcMeta.class);
  public static final String ORG_PROPERTY_KEY = FineoJdbcProperties.COMPANY_KEY_PROPERTY;
  private FineoSqlRewriter rewrite;
  private String org;

  private final Cache<String, Map<String, String>> connectionPropertyCache;

  public FineoJdbcMeta(String url, Properties info, MetricsSystem metrics, String org) throws
    SQLException {
    this(url, info, metrics);
    LOG.info("Connecting to drill at: {} for org: {}", url, org);
    setRewriter(org);
  }

  public FineoJdbcMeta(String url, Properties info, MetricsSystem metrics) throws
    SQLException {
    super(url, info, metrics);

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
    try {
      super.openConnection(ch, info);
    } catch (RuntimeException e) {
      LOG.error("Failed to open connection: {}", ch, e);
      throw e;
    }

    this.connectionPropertyCache.put(ch.id, info);
  }

  private String getOrg(ConnectionHandle ch) {
    return getOrg(ch.id);
  }

  private String getOrg(String cid) {
    Map<String, String> props = checkNotNull(connectionPropertyCache.getIfPresent(cid),
      "No connection key found for connection handle '%s'", cid);

    return props.get(ORG_PROPERTY_KEY);
  }

  //
  // Write Operations
  //

  @Override
  public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
    String org = getOrg(ch);
    return super.prepare(ch, rewrite(sql, org), maxRowCount);
  }

  // Superclass calls into the prepareAndExecute that we have implemented, so save the effort of
  // rewriting here
//  @Override
//  public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount,
//    PrepareCallback callback) throws NoSuchStatementException {
//    String org = getOrg(h.connectionId);
//    return super.prepareAndExecute(h, rewrite(sql, org), maxRowCount, callback);
//  }

  @Override
  public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount,
    int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {

    return super.prepareAndExecute(h, rewrite(sql, org), maxRowCount, maxRowsInFirstFrame,
      callback);
  }

  private String rewrite(String sql, String org) {
    Preconditions.checkArgument(this.org.equals(org),
      "Routing error! Customer got routed to the wrong JDBC server.");
    try {
      String wrote = rewrite.rewrite(sql);
      return wrote;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public ResultSet prepareAndExecuteQuery(StatementHandle h, String sql, long maxRowCount) throws
    NoSuchStatementException, SQLException {
    final StatementInfo info = getStatementCache().getIfPresent(h.id);
    if (info == null) {
      throw new NoSuchStatementException(h);
    }
    final Statement statement = info.statement;
    // Make sure that we limit the number of rows for the query
    setMaxRows(statement, maxRowCount);
    String org = getOrg(h.connectionId);
    sql = rewrite(sql, org);
    boolean out = statement.execute(sql);
    info.setResultSet(statement.getResultSet());
    // Either execute(sql) returned true or the resultSet was null
    assert out || null == info.getResultSet();
    return info.getResultSet();
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

  public Connection getConnection(ConnectionHandle handle) throws SQLException {
    return super.getConnection(handle.id);
  }

  public void setRewriter(String org) {
    this.rewrite = new FineoSqlRewriter(org);
    this.org = org;
  }

  @VisibleForTesting
  public void setRewriterForTesting(FineoSqlRewriter rewrite) {
    this.rewrite = rewrite;
  }
}
