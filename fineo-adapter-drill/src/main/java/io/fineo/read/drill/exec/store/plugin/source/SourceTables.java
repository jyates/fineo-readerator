package io.fineo.read.drill.exec.store.plugin.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

@JsonTypeName("tables")
public class SourceTables {
  private final List<SourceTable> tables;

  public SourceTables(@JsonProperty("tables") List<SourceTable> tables) {
    this.tables = tables;
  }

  @JsonProperty("tables")
  public List<SourceTable> getTables() {
    return tables;
  }
}
