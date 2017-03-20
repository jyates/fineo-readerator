package io.fineo.read.drill.e2e.command;

import io.fineo.read.Driver;
import io.fineo.read.drill.e2e.options.DrillArguments;
import io.fineo.read.jdbc.FineoConnectionProperties;

import java.util.Properties;

public class FineoLocalRead extends Reader {

  public FineoLocalRead() {
    super("io.fineo.read.Driver");
  }

  @Override
  public Properties loadProperties(DrillArguments opts) {
    Properties props = new Properties();
    // no authentication - just an API key
    props.put(FineoConnectionProperties.API_KEY.camelName(), opts.org.get());
    // no prefix for the api. Ensures that requests just hit url/, rather than url/prod (which we
    // don't serve from the local rest server
    props.put("fineo.internal.test.api-prefix", "/");
    props.put("user", opts.org.get());
    return props;
  }

  @Override
  public String getJdbcConnection(String url) {
    return Driver.CONNECT_PREFIX + url;
  }
}
