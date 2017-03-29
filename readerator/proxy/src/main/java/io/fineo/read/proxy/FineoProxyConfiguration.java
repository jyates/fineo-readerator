package io.fineo.read.proxy;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

public class FineoProxyConfiguration extends Configuration {

  @NotEmpty
  private String jdbcUrl;
  private Integer jdbcPortFix = 0;

  @JsonProperty
  public String getJdbcUrl() {
    return jdbcUrl;
  }

  @JsonProperty
  public FineoProxyConfiguration setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
    return this;
  }

  @JsonProperty
  public Integer getJdbcPortFix() {
    return jdbcPortFix;
  }

  @JsonProperty
  public FineoProxyConfiguration setJdbcPortFix(Integer jdbcPortFix) {
    this.jdbcPortFix = jdbcPortFix;
    return this;
  }
}
