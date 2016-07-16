package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Track the key and attribute filters for a scan
 */
@JsonTypeName("dynamo-read-filter-spec")
@JsonAutoDetect
public class DynamoReadFilterSpec {

  private DynamoFilterSpec keyFilter;

  @JsonCreator
  public DynamoReadFilterSpec(@JsonProperty("key") DynamoFilterSpec keyFilter) {
    this.keyFilter = keyFilter;
  }

  public DynamoReadFilterSpec() {
    keyFilter = new DynamoFilterSpec();
  }

  @JsonProperty("keyFilter")
  public DynamoFilterSpec getKeyFilter() {
    return keyFilter;
  }
}
