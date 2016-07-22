package io.fineo.read.drill.exec.store.plugin;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("schema-repository")
public class SchemaRepository {

  private final String table;

  public SchemaRepository(@JsonProperty("table") String table) {
    this.table = table;
  }

  @JsonProperty("table")
  public String getTable() {
    return table;
  }
}
