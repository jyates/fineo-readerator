package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Track the key and attribute filters for a scan
 */
@JsonTypeName("dynamo-scan-filter-spec")
@JsonAutoDetect
public class DynamoScanFilterSpec {

  private boolean hasHashKey;
  private boolean hasRangeKey;
  private DynamoFilterSpec keyFilter;
  private DynamoFilterSpec attributeFilter;

  @JsonCreator
  public DynamoScanFilterSpec(@JsonProperty("key") DynamoFilterSpec keyFilter,
    @JsonProperty("hasHashKey") boolean hasHashKey,
    @JsonProperty("hasRangeKey") boolean hasRangeKey,
    @JsonProperty("attr") DynamoFilterSpec attributeFilter) {
    this.keyFilter = keyFilter;
    this.hasHashKey = hasHashKey;
    this.attributeFilter = attributeFilter;
    this.hasRangeKey = hasRangeKey;
  }

  public DynamoScanFilterSpec() {
  }

  public DynamoFilterSpec getKeyFilter() {
    return keyFilter;
  }

  public DynamoFilterSpec getAttributeFilter() {
    return attributeFilter;
  }

  public boolean getHasHashKey() {
    return hasHashKey;
  }

  public boolean getHasRangeKey() {
    return hasRangeKey;
  }
}
