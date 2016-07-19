package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec;

/**
 * Track the key and attribute filters for a scan
 */
@JsonAutoDetect
@JsonTypeName("dynamo-read-filter-spec")
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@class")
public class DynamoReadFilterSpec {

  protected DynamoFilterSpec key;

  @JsonCreator
  public DynamoReadFilterSpec(@JsonProperty("key") DynamoFilterSpec key) {
    this.key = key;
  }

  public DynamoReadFilterSpec() {
    key = new DynamoFilterSpec();
  }

  @JsonProperty("key")
  public DynamoFilterSpec getKey() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof DynamoReadFilterSpec))
      return false;

    DynamoReadFilterSpec that = (DynamoReadFilterSpec) o;

    return getKey() != null ? getKey().equals(that.getKey()) : that.getKey() == null;

  }

  @Override
  public int hashCode() {
    return getKey() != null ? getKey().hashCode() : 0;
  }

  @Override
  public String toString() {
    return "DynamoReadFilterSpec{" +
           "key=" + key +
           '}';
  }
}
