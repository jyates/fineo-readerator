package io.fineo.read.proxy;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

public class FineoProxyConfiguration extends Configuration {

  @NotEmpty
  private String jdbcUrl;

  @JsonProperty
  public String getJdbcUrl() {
    return jdbcUrl;
  }

  @JsonProperty
  public FineoProxyConfiguration setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
    return this;
  }
}
