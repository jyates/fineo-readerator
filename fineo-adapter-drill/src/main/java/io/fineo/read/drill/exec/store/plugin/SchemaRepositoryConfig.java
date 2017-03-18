package io.fineo.read.drill.exec.store.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
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

  @JsonIgnore
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof SchemaRepositoryConfig))
      return false;

    SchemaRepositoryConfig that = (SchemaRepositoryConfig) o;

    return getTable() != null ? getTable().equals(that.getTable()) : that.getTable() == null;

  }

  @JsonIgnore
  @Override
  public int hashCode() {
    return getTable() != null ? getTable().hashCode() : 0;
  }
}
