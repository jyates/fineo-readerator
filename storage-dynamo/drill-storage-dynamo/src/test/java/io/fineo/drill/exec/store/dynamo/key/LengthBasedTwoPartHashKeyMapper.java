package io.fineo.drill.exec.store.dynamo.key;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple mapper that just converts a compound key into length-based substring parts.
 */
public class LengthBasedTwoPartHashKeyMapper extends DynamoKeyMapper {

  private final int length;

  @JsonCreator
  public LengthBasedTwoPartHashKeyMapper(@JsonProperty("length") int length,
    @JacksonInject DynamoKeyMapperSpec spec) {
    super(spec);
    this.length = length;
  }

  @Override
  @JsonIgnore
  public Map<String, Object> mapHashKey(Object value) {
    String val = (String) value;
    String p1 = val.substring(0, length);
    String p2 = val.substring(length);
    Map<String, Object> out = new HashMap<>();
    out.put(spec.getKeyNames().get(0), p1);
    out.put(spec.getKeyNames().get(1), p2);
    return out;
  }

  @Override
  @JsonIgnore
  public Map<String, Object> mapSortKey(Object value) {
    throw new UnsupportedOperationException("No mapping for sort key!");
  }

  @JsonProperty("length")
  public int getLength() {
    return length;
  }
}
