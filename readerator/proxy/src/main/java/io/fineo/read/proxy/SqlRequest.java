package io.fineo.read.proxy;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class SqlRequest {
  private String sql;

  @JsonProperty("sql")
  public String getSql() {
    return sql;
  }

  @JsonProperty("Sql")
  public SqlRequest setSql(String sql) {
    this.sql = sql;
    return this;
  }
}
