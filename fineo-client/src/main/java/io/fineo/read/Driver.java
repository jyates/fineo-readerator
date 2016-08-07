package io.fineo.read;

import io.fineo.read.http.FineoAvaticaAwsHttpClient;
import io.fineo.read.jdbc.FineoConnectionProperties;
import io.fineo.read.jdbc.connection.RemoteMeta;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.avatica.remote.AvaticaHttpClient;
import org.apache.calcite.avatica.remote.MockJsonService;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.RemoteProtobufService;
import org.apache.calcite.avatica.remote.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Driver extends org.apache.calcite.avatica.remote.Driver {
  private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

  private static final String CONNECT_PREFIX = "jdbc:fineo:";

  static {
    try {
      new Driver().register();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public Driver() throws ClassNotFoundException {
    super();
    Class.forName(org.apache.calcite.avatica.remote.Driver.class.getName());
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

  /**
   * Returns the name of a class to be factory for JDBC objects
   * (connection, statement) appropriate for the current JDBC version.
   */
  protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
      case JDBC_30:
      case JDBC_40:
        throw new IllegalArgumentException("JDBC version not supported: "
                                           + jdbcVersion);
      case JDBC_41:
      default:
        return "io.fineo.read.jdbc.connection.FineoJdbc41Factory";
    }
  }

  @Override
  protected Collection<ConnectionProperty> getConnectionProperties() {
    final List<ConnectionProperty> list = new ArrayList<ConnectionProperty>();
    Collections.addAll(list, FineoConnectionProperties.values());
    return list;
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    AvaticaConnection conn = (AvaticaConnection) super.connect(url, info);
    if (conn == null) {
      // It's not an url for our driver
      return null;
    }

    // Create the corresponding remote connection
    ConnectionConfig config = conn.config();
    Service service = createService(conn, config);

    service.apply(
      new Service.OpenConnectionRequest(conn.id,
        Service.OpenConnectionRequest.serializeProperties(info)));

    return conn;
  }

  private Properties convertProperties(Properties info){
    // ensure we use our factory and client
  }
}
