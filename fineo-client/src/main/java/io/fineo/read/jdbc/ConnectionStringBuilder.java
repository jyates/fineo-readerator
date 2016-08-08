package io.fineo.read.jdbc;

import com.google.common.base.Joiner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConnectionStringBuilder {

  private static final Joiner AND = Joiner.on("&");
  private final StringBuffer sb;
  private final List<String> props;

  public ConnectionStringBuilder(String url, String target) {
    this.sb = new StringBuffer(url);
    sb.append("url=" + target);
    this.props = new ArrayList<>();
  }

  public String build() {
    if (props.size() == 0) {
      return sb.toString();
    }
    // start query section
    sb.append("?");
    try {
      return AND.appendTo(sb, props).toString();
    } catch (IOException e) {
      throw new RuntimeException("Unexpected error when building connection url!");
    }
  }

  public ConnectionStringBuilder withInt(FineoConnectionProperties prop, Properties info) {
    String value = getValue(prop, info);
    // skip unset
    if (!value.equals("-1")) {
      return with(prop.camelName(), value);
    }
    return this;
  }

  public ConnectionStringBuilder with(FineoConnectionProperties prop, Properties info) {
    String value = getValue(prop, info);
    return with(prop.camelName(), value);
  }

  private String getValue(FineoConnectionProperties prop, Properties info) {
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
    return value;
  }

  public ConnectionStringBuilder with(String key, String value) {
    props.add(key + "=" + value);
    return this;
  }

  public static Map<String, String> parse(java.net.URL url) {
    Map<String, String> map = new HashMap<>();
    String query = url.getQuery();
    String[] parts = query.split("&");
    for (String part : parts) {
      String[] kv = part.split("=");
      map.put(kv[0], kv[1]);
    }
    return map;
  }
}
