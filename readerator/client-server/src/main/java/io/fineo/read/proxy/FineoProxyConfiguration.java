package io.fineo.read.proxy;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import javax.validation.constraints.NotNull;

/**
 *
 */
public class FineoProxyConfiguration extends Configuration {

  private String jdbcUrl;

  @JsonProperty("jdbcUrl")
  public String getJdbcUrl() {
    return jdbcUrl;
  }

  @JsonProperty("jdbcUrl")
  public FineoProxyConfiguration setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
    return this;
  }
}
