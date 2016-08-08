package io.fineo.read;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import io.fineo.read.jdbc.FineoConnectionProperties;
import io.fineo.read.jdbc.FineoHttpClientFactory;
import io.fineo.read.jdbc.SystemPropertyPassThroughUtil;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.calcite.avatica.BuiltInConnectionProperty.HTTP_CLIENT_FACTORY;

public class Driver extends org.apache.calcite.avatica.remote.Driver {
  private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

  private static final String CONNECT_PREFIX = "jdbc:fineo:";
  private static final String AUTH_TYPE_SEPARATOR = "_OR_";

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
    return super.connect(url, convertProperties(info));
  }

  private Properties convertProperties(Properties info) {
    // ensure we use our factory to create our client
    info.put(HTTP_CLIENT_FACTORY.camelName(), FineoHttpClientFactory.class.getName());
    // yup, always use protobuf
    info.put(BuiltInConnectionProperty.SERIALIZATION, "PROTOBUF");
    setupAuthentication(info);
    setupClientProperties(info);
    return info;
  }

  /**
   * pull out the client/connection properties into the system, since we can't get an instance
   * of the properties in the client proper... yeah, come on avatica
   */
  private void setupClientProperties(Properties info) {
    set(FineoConnectionProperties.CLIENT_EXEC_TIMEOUT, info);
    set(FineoConnectionProperties.CLIENT_MAX_CONNECTIONS, info);
    set(FineoConnectionProperties.CLIENT_MAX_IDLE, info);
    set(FineoConnectionProperties.CLIENT_REQUEST_TIMEOUT, info);
    set(FineoConnectionProperties.CLIENT_INIT_TIMEOUT, info);
    set(FineoConnectionProperties.CLIENT_TTL, info);
  }

  private void set(FineoConnectionProperties prop, Properties info) {
    String value;
    switch (prop.type()) {
      case STRING:
        value = prop.wrap(info).getString();
        break;
      case BOOLEAN:
        value = Boolean.toString(prop.wrap(info).getBoolean());
        break;
      case NUMBER:
        value = Long.toString(prop.wrap(info).getLong());
        break;
      case ENUM:
      default:
        throw new UnsupportedOperationException(
          "Cannot set an " + prop.type() + " client property!");
    }
    SystemPropertyPassThroughUtil.set(prop, value);
  }

  private void setupAuthentication(Properties info) {
    // api key has to be specified
    set(FineoConnectionProperties.API_KEY, info);
    // load all the places the credentials could be stored
    AWSCredentialsProviderChain chain = loadCredentialChain(info);
    String user = chain.getCredentials().getAWSAccessKeyId();
    String password = chain.getCredentials().getAWSSecretKey();
    info.setProperty(BuiltInConnectionProperty.AVATICA_USER.camelName(), user);
    info.setProperty(BuiltInConnectionProperty.AVATICA_PASSWORD.camelName(), password);
  }

  private AWSCredentialsProviderChain loadCredentialChain(Properties info) {
    String authType = FineoConnectionProperties.AUTHENTICATION.wrap(info).getString();
    String[] types = authType.split(AUTH_TYPE_SEPARATOR);
    List<AWSCredentialsProvider> sources = new ArrayList<>();
    for (String type : types) {
      switch (type.toLowerCase()) {
        case "default":
          return new DefaultAWSCredentialsProviderChain();
        case "static":
          String key = FineoConnectionProperties.AWS_KEY.wrap(info).getString();
          String secret = FineoConnectionProperties.AWS_KEY.wrap(info).getString();
          sources.add(new StaticCredentialsProvider(new BasicAWSCredentials(key, secret)));
          break;
        case "system":
          sources.add(new SystemPropertiesCredentialsProvider());
          break;
        case "env":
          sources.add(new EnvironmentVariableCredentialsProvider());
          break;
        case "profile":
          sources.add(new ProfileCredentialsProvider(FineoConnectionProperties
            .PROFILE_CREDENTIAL_NAME.wrap(info).getString()));
          break;
        case "profile_inst":
          sources.add(new InstanceProfileCredentialsProvider());
          break;
        default:
          LOG.warn("No authentication provider of type {} supported!", type);
      }
    }

    return new AWSCredentialsProviderChain(sources);
  }
}
