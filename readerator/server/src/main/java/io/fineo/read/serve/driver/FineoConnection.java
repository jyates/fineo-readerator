package io.fineo.read.serve.driver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fineo.read.FineoJdbcProperties;
import io.fineo.read.serve.FineoServer;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.jdbc.FineoJdbcMeta;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Basically override all the functionality in the avatica connection that relies on having a
 * Meta instance
 */
public class FineoConnection extends AvaticaConnection {

  private final String org;
  private final String catalog;

  /**
   * Creates an AvaticaConnection.
   * <p>
   * <p>Not public; method is called only from the driver or a derived
   * class.</p>
   *
   * @param driver  Driver
   * @param factory Factory for JDBC objects
   * @param url     Server URL
   * @param info    Other connection properties
   */
  protected FineoConnection(FineoServerDriver driver,
    FineoJdbc41Factory factory, String url, Properties info) throws SQLException {
    //TODO why can't we copy the info here for HsqlDB?
    super(driver, factory, url, info);
    this.org = Preconditions.checkNotNull(info.getProperty(FineoJdbcProperties.COMPANY_KEY_PROPERTY),
      "No org specified when creating connection!");
    // kind of an ugly hack - we want to pass in the catalog we are using in Drill. However, this
    // is really should be the catalog of this connection, but its accessed from the external
    // facing FineoDatabaseMetaData as the catalog that the real underlying connection should
    // use... confusing, I know. See TestFineoServerDriver#testGetTables for how this works out
    this.catalog = Preconditions.checkNotNull(info.getProperty(FineoServer.DRILL_CATALOG_KEY));

    Map<String, String> props = new HashMap<>();
    for (String name : info.stringPropertyNames()) {
      props.put(name, info.getProperty(name));
    }
    this.meta.openConnection(this.handle, props);
  }

  private static Properties cloneAndOverrideProperties(Properties info) {
    // have to copy everything over because defaults aren't respected everywhere
    Properties copy = new Properties();
    for (String name : info.stringPropertyNames()) {
      copy.put(name, info.getProperty(name));
    }
    copy.put(AvaticaConnection.NUM_EXECUTE_RETRIES_KEY, 1);
    return copy;
  }

  public String getOrg() {
    return org;
  }

  @Override
  public String getCatalog() {
    return catalog;
  }

  public Properties getInfo() {
    return info;
  }

  @Override
  public String getSchema() {
    return FineoDatabaseMetaData.FINEO_SCHEMA;
  }

  public Connection getMetaConnection() throws SQLException {
    return ((FineoJdbcMeta) this.meta).getConnection(handle);
  }

  @Override
  protected Meta.ExecuteResult prepareAndExecuteInternal(AvaticaStatement statement, String sql,
    long maxRowCount) throws SQLException, NoSuchStatementException {
    throw new UnsupportedOperationException("We don't provide an ExecuteResult. Instead use "
                                            + "#prepareAndExecute(FineoStatement)");
  }

  ResultSet prepareAndExecute(FineoStatement statement, String sql, long maxRowCount)
    throws NoSuchStatementException, SQLException {
    return ((FineoJdbcMeta)meta).prepareAndExecuteQuery(statement.handle, sql, maxRowCount);
  }

  //  @Override
//  public void commit() throws SQLException {
//    meta.commit(handle);
//  }
//
//  @Override
//  public void rollback() throws SQLException {
//    meta.rollback(handle);
//  }
//
//
//  @Override
//  public void close() throws SQLException {
//    if (closed) {
//      return;
//    }
//    delegate.close();
//    closed = true;
//
//    // Per specification, if onConnectionClose throws, this method will throw
//    // a SQLException, but statement will still be closed.
//    try {
//      driver.handler.onConnectionClose(this);
//    } catch (RuntimeException e) {
//      throw helper.createException("While closing connection", e);
//    }
//  }

  @VisibleForTesting
  FineoJdbcMeta getMetaForTesting(){
    return (FineoJdbcMeta) this.meta;
  }
}
