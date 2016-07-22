package io.fineo.read.drill.exec.store.plugin.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Enable the dynamo source. Instead of configuring everything here, we just leverage the source
 * to provide the list of tables that we actually want to scan
 */
@JsonTypeName(DynamoSourceTable.NAME)
public class DynamoSourceTable extends SourceTable {
  @JsonIgnore
  public static final String NAME = "dynamo-source";
  @JsonIgnore
  private static final String SCHEMA = "dynamo";

  private final String pattern;

  public DynamoSourceTable(@JsonProperty("pattern") String pattern) {
    super(SCHEMA);
    this.pattern = pattern;
  }

  @JsonProperty("pattern")
  public String getPattern() {
    return pattern;
  }
}
