package io.fineo.read.serve.driver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fineo.read.FineoJdbcProperties;
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
    super(driver, factory, url, setUser(info));
    // we just overrode the user, to orgID, so we just query as that user
    this.org = info.getProperty("user");

    Map<String, String> props = new HashMap<>();
    for (String name : info.stringPropertyNames()) {
      props.put(name, info.getProperty(name));
    }
    // open the internal connection and leave it open
    this.meta.openConnection(this.handle, props);
  }

  private static Properties setUser(Properties info) {
    String org =
      Preconditions.checkNotNull(info.getProperty(FineoJdbcProperties.COMPANY_KEY_PROPERTY),
        "No org specified when creating connection!");
    info.setProperty("user", org);
    return info;
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

  public Properties getInfo() {
    return info;
  }

  @Override
  protected Meta.ExecuteResult prepareAndExecuteInternal(AvaticaStatement statement, String sql,
    long maxRowCount) throws SQLException, NoSuchStatementException {
    throw new UnsupportedOperationException("We don't provide an ExecuteResult. Instead use "
                                            + "#prepareAndExecute(FineoStatement)");
  }

  ResultSet prepareAndExecute(FineoStatement statement, String sql, long maxRowCount)
    throws NoSuchStatementException, SQLException {
    return ((FineoJdbcMeta) meta).prepareAndExecuteQuery(statement.handle, sql, maxRowCount);
  }

  // fixed properties
  // ----------------

  @Override
  public String getCatalog() {
    return FineoDatabaseMetaData.FINEO_CATALOG;
  }

  @Override
  public String getSchema() {
    return FineoDatabaseMetaData.FINEO_SCHEMA;
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  public Connection getMetaConnection() throws SQLException {
    return ((FineoJdbcMeta) this.meta).getConnection(handle);
  }

  @VisibleForTesting
  FineoJdbcMeta getMetaForTesting() {
    return (FineoJdbcMeta) this.meta;
  }
}
