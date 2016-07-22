package io.fineo.drill.exec.store.dynamo.key;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Map;

/**
 * Base class for a key mapper. All key-mapping implementations should inherit from this class
 */
@JsonTypeName("dynamo-key-mapper-impl")
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public abstract class DynamoKeyMapper {

  protected final DynamoKeyMapperSpec spec;

  @JsonCreator
  protected DynamoKeyMapper(@JacksonInject DynamoKeyMapperSpec spec) {
    this.spec = spec;
  }

  @JsonIgnore
  public Map<String, Object> mapHashKey(Object value) {
    return null;
  }

  @JsonIgnore
  public Map<String, Object> mapSortKey(Object value) {
    return null;
  }
}
