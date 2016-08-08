package io.fineo.read.drill.e2e.command;

import io.fineo.read.drill.e2e.options.DrillArguments;

import java.util.Properties;

/**
 *
 */
public abstract class Reader {
  private final String driver;

  protected Reader(String driver) {
    this.driver = driver;
  }

  public String getDriver() {
    return driver;
  }

  public abstract String getJdbcConnection(String url);

  public Properties loadProperties(DrillArguments opts){
    Properties props = new Properties();
    props.put("COMPANY_KEY", opts.org.get());
    return props;
  }
}
