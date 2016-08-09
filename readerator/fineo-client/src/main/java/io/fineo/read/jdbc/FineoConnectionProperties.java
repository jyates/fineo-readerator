package io.fineo.read.jdbc;

import org.apache.calcite.avatica.ConnectionConfigImpl;
import org.apache.calcite.avatica.ConnectionProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.calcite.avatica.ConnectionConfigImpl.parse;

public enum FineoConnectionProperties implements ConnectionProperty {

  API_KEY("api_key", Type.STRING, null, true),
  AUTHENTICATION("authentication", Type.STRING, "default", false),
  /**
   * Static credential config
   */
  AWS_KEY("aws_key", Type.STRING, null, false),
  AWS_SECRET("aws_secret", Type.STRING, null, false),

  /**
   * Profile credential config
   */
  PROFILE_CREDENTIAL_NAME("profile_name", Type.STRING, null, false),

  /**
   * Client connection configs
   */
  // time initially establishing a connection before giving up and timing out.
  CLIENT_INIT_TIMEOUT("client_init_connection_timeout_millis"),
  // maximum number of allowed open HTTP connections
  CLIENT_MAX_CONNECTIONS("client_max_connections"),
  // time for the request to complete before giving up and timing out.
  CLIENT_REQUEST_TIMEOUT("client_request_timeout_millis"),
  // number of retries for a single http request before giving up
  CLIENT_MAX_ERROR_RETRY("client_request_max_retries");

  private final String camelName;
  private final Type type;
  private final Object defaultValue;
  private final boolean required;

  private static final Map<String, FineoConnectionProperties> NAME_TO_PROPS;

  static {
    NAME_TO_PROPS = new HashMap<>();
    for (FineoConnectionProperties p : FineoConnectionProperties.values()) {
      NAME_TO_PROPS.put(p.camelName.toUpperCase(), p);
      NAME_TO_PROPS.put(p.name(), p);
    }
  }

  /**
   * Helper for number constructor
   *
   * @param name
   */
  FineoConnectionProperties(String name) {
    this(name, Type.NUMBER, -1, false);
  }

  FineoConnectionProperties(String camelName, Type type, Object defaultValue,
    boolean required) {
    this.camelName = camelName;
    this.type = type;
    this.defaultValue = defaultValue;
    this.required = required;
    assert defaultValue == null || type.valid(defaultValue);
  }

  @Override
  public String camelName() {
    return camelName;
  }

  @Override
  public Object defaultValue() {
    return defaultValue;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public ConnectionConfigImpl.PropEnv wrap(Properties properties) {
    return new ConnectionConfigImpl.PropEnv(parse(properties, NAME_TO_PROPS), this);
  }

  @Override
  public boolean required() {
    return required;
  }
}
