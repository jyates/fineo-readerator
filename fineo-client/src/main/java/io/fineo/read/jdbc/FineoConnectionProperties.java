package io.fineo.read.jdbc;

import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionConfigImpl;
import org.apache.calcite.avatica.ConnectionProperty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.calcite.avatica.ConnectionConfigImpl.parse;

/**
 *
 */
public enum FineoConnectionProperties implements ConnectionProperty{

  AUTHENTICATION("authentication", Type.STRING, null, false);

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
