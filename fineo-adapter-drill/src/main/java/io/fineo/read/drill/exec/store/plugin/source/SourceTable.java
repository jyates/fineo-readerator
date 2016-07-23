package io.fineo.read.drill.exec.store.plugin.source;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Base class for the source table for Fineo to read
 */
@JsonAutoDetect
@JsonTypeName("source")
public abstract class SourceTable {

  protected final String schema;

  public SourceTable(@JsonProperty("schema") String schema) {
    this.schema = schema;
  }

  @JsonProperty("schema")
  public String getSchema() {
    return schema;
  }
}
