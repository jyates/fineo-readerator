package io.fineo.drill.exec.store.dynamo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Map;

@JsonTypeName("dynamo-table")
public class DynamoTableDefinition {

  private final String name;
  private final Map<String, String> pkToType;

  @JsonCreator
  public DynamoTableDefinition(@JsonProperty("name") String name,
    @JsonProperty("pkToType") Map<String, String>
    pkToType) {
    this.name = name;
    this.pkToType = pkToType;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public Map<String, String> getPkToType() {
    return pkToType;
  }
}
