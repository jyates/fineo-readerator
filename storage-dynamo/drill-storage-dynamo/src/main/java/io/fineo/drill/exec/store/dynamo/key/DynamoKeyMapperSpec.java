package io.fineo.drill.exec.store.dynamo.key;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;
import java.util.Map;

/**
 * Specify a key mapper class
 */
@JsonTypeName(DynamoKeyMapperSpec.NAME)
public class DynamoKeyMapperSpec {
  public static final String NAME = "dynamo-key-mapper";
  private final List<String> keyNames;
  private final List<String> keyValues;
  private final Map<String, Object> args;

  public DynamoKeyMapperSpec(@JsonProperty("key-name") List<String> keyName,
    @JsonProperty("key-type") List<String> keyValue,
    @JsonProperty("args") Map<String, Object> args) {
    this.keyNames = keyName;
    this.keyValues = keyValue;
    this.args = args;
  }

  @JsonProperty("key-name")
  public List<String> getKeyNames() {
    return keyNames;
  }

  @JsonProperty("key-value")
  public List<String> getKeyValues() {
    return keyValues;
  }

  @JsonProperty("args")
  public Map<String, Object> getArgs() {
    return args;
  }
}
