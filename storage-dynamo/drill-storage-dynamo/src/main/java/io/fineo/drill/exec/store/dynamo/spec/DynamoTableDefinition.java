package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;
import java.util.Map;

@JsonTypeName("dynamo-table")
public class DynamoTableDefinition {

  private final String name;
  private final List<PrimaryKey> keys;

  @JsonCreator
  public DynamoTableDefinition(@JsonProperty("name") String name,
    @JsonProperty("keys") List<PrimaryKey> keys) {
    this.name = name;
    this.keys = keys;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public List<PrimaryKey> getKeys() {
    return keys;
  }

  @JsonTypeName("dynamo-key-def")
  public static class PrimaryKey {
    private final String name;
    private String type;
    private final boolean isHashKey;

    @JsonCreator
    public PrimaryKey(@JsonProperty("name") String name, @JsonProperty("type") String type,
      @JsonProperty("isHashKey") boolean isHashKey) {
      this.name = name;
      this.type = type;
      this.isHashKey = isHashKey;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public boolean isHashKey() {
      return isHashKey;
    }

    public void setType(String type) {
      this.type = type;
    }
  }
}
