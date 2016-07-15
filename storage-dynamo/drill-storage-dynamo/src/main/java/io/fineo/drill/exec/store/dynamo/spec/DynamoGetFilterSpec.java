package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;


@JsonTypeName("dynamo-get-filter-spec")
public class DynamoGetFilterSpec extends DynamoReadFilterSpec {
  @JsonCreator
  public DynamoGetFilterSpec(@JsonProperty("key") DynamoFilterSpec keyFilter) {
    super(keyFilter, null);
  }
}
