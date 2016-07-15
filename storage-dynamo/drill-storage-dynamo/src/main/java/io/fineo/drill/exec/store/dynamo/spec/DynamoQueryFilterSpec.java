package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;


@JsonTypeName("dynamo-get-filter-spec")
public class DynamoQueryFilterSpec extends DynamoReadFilterSpec {
  @JsonCreator
  public DynamoQueryFilterSpec(@JsonProperty("key") DynamoFilterSpec keyFilter,
    @JsonProperty("attr") DynamoFilterSpec attributeFilter) {
    super(keyFilter, attributeFilter);
  }
}
