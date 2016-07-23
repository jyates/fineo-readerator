package io.fineo.read.drill.exec.store.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(SchemaRepositoryConfig.NAME)
public class SchemaRepositoryConfig {

  public static final String NAME = "schema-repository";
  private final String table;

  @JsonCreator
  public SchemaRepositoryConfig(@JsonProperty("table") String table) {
    this.table = table;
  }

  @JsonProperty("table")
  public String getTable() {
    return table;
  }
}
