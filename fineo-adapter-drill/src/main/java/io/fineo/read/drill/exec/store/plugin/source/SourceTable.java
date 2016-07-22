package io.fineo.read.drill.exec.store.plugin.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Base class for the source table for Fineo to read
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public abstract class SourceTable {

  protected final String schema;

  public SourceTable(@JsonProperty("schema") String schema) {
    this.schema = schema;
  }

  @JsonProperty
  public String getSchema() {
    return schema;
  }
}
