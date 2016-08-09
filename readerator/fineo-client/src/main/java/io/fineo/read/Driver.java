package io.fineo.read;

import com.google.common.base.Preconditions;
import io.fineo.read.http.FineoAvaticaAwsHttpClient;
import io.fineo.read.jdbc.ConnectionStringBuilder;
import io.fineo.read.jdbc.FineoConnectionProperties;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.fineo.read.jdbc.AuthenticationUtil.setupAuthentication;
import static io.fineo.read.jdbc.FineoConnectionProperties.API_KEY;
import static org.apache.calcite.avatica.BuiltInConnectionProperty.HTTP_CLIENT_IMPL;
import static org.apache.calcite.avatica.BuiltInConnectionProperty.SERIALIZATION;
import static org.apache.calcite.avatica.remote.Driver.Serialization.PROTOBUF;

public class Driver extends org.apache.calcite.avatica.remote.Driver {

  public static final String CONNECT_PREFIX = "jdbc:fineo:";

  private static final String URL = "https://53r0nhslih.execute-api.us-east-1.amazonaws.com/prod";

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

  @Override
  protected Collection<ConnectionProperty> getConnectionProperties() {
    final List<ConnectionProperty> list = new ArrayList<ConnectionProperty>();
    Collections.addAll(list, FineoConnectionProperties.values());
    return list;
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    try {
      // do the same parsing as in UnregisteredDriver#connect(...). We so this here so we can
      // generate a URL that contains all the necessary properties, allowing them to get passed
      // to our custom client. The alternative, right now, is to create a custom factory and have
      // that create custom Avatica connections, which have our metadata info
      final String prefix = getConnectStringPrefix();
      assert url.startsWith(prefix);
      final String urlSuffix = url.substring(prefix.length());
      final Properties info2 = ConnectStringParser.parse(urlSuffix, info);
      String updatedUrl = convertProperties(info2);
      return super.connect(updatedUrl, info);
    } catch (IOException e) {
      throw new SQLException("Unexpected exception while obtaining connection!");
    }
  }

  private String convertProperties(Properties info) throws IOException {
    // ensure we use our factory to create our client
    info.put(HTTP_CLIENT_IMPL.camelName(), FineoAvaticaAwsHttpClient.class.getName());
    // yup, always use protobuf
    info.put(SERIALIZATION.camelName(), PROTOBUF.toString());
    setupAuthentication(info);

    // properties that are passed through the connection string
    ConnectionStringBuilder sb = new ConnectionStringBuilder(getConnectStringPrefix(),
      BuiltInConnectionProperty.URL.wrap(info).getString(URL));
    String key = Preconditions
      .checkNotNull(API_KEY.wrap(info).getString(), "Must specify the Fineo API Key via %s",
        API_KEY.camelName());
    sb.with(API_KEY, info);
    // API KEY is also the company key, so set that too
    info.put(FineoProperties.COMPANY_KEY_PROPERTY, key);
    setupClientProperties(info, sb);
    return sb.build();
  }

  /**
   * pull out the client/connection properties into the system, since we can't get an instance
   * of the properties in the client proper... yeah, come on avatica
   */
  private void setupClientProperties(Properties info, ConnectionStringBuilder sb) {
    sb.withInt(FineoConnectionProperties.CLIENT_INIT_TIMEOUT, info)
      .withInt(FineoConnectionProperties.CLIENT_MAX_CONNECTIONS, info)
      .withInt(FineoConnectionProperties.CLIENT_REQUEST_TIMEOUT, info)
      .withInt(FineoConnectionProperties.CLIENT_MAX_ERROR_RETRY, info);
  }
}
