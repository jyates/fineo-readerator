package io.fineo.read.serve.driver;

import io.fineo.read.FineoJdbcProperties;
import io.fineo.read.serve.FineoServer;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.avatica.jdbc.FineoJdbcMeta;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Custom driver for the server-side JDBC connections. Ensures that we don't leak information
 * about other tables/users to the current connection
 */
public class FineoServerDriver extends UnregisteredDriver {

  public static final String CONNECT_PREFIX = "jdbc:fineo-server:";

  static {
    try {
      new FineoServerDriver().register();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean load() {
    return true;
  }

  private final MetricsSystem metrics;

  public FineoServerDriver() throws ClassNotFoundException {
    this(null);
  }

  public FineoServerDriver(MetricsSystem metricsSystem) throws ClassNotFoundException {
    super();
    // make sure we can reach drill as a delegate connection
    Class.forName("org.apache.drill.jdbc.Driver");
    this.metrics = metricsSystem == null ? NoopMetricsSystem.getInstance() : metricsSystem;
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
      org.apache.calcite.avatica.remote.Driver.class,
      "io-fineo-jdbc.properties",
      "Fineo JDBC Driver",
      "unknown version",
      "Fineo",
      "unknown version");
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_PREFIX;
  }

  @Override
  public Meta createMeta(AvaticaConnection connection) {
    FineoConnection conn = (FineoConnection) connection;
    Properties props = conn.getInfo();
    String url = props.getProperty(FineoServer.DRILL_CONNECTION_KEY);
    String org = props.getProperty(FineoJdbcProperties.COMPANY_KEY_PROPERTY);
    try {
      return new FineoJdbcMeta(url, props, metrics, org);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the name of a class to be factory for JDBC objects
   * (connection, statement) appropriate for the current JDBC version.
   */
  @Override
  protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
      case JDBC_30:
      case JDBC_40:
        throw new IllegalArgumentException("JDBC version not supported: " + jdbcVersion);
      case JDBC_41:
      default:
        return "io.fineo.read.serve.driver.FineoJdbc41Factory";
    }
  }
}
