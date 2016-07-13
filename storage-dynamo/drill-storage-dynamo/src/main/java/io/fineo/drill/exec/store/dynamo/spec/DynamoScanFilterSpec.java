package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("dynamo-scan-filter-spec")
@JsonAutoDetect
public class DynamoScanFilterSpec {

  private DynamoFilterSpec hashKeyFilter;
  private DynamoFilterSpec rangeKeyFilter;
  private DynamoFilterSpec attributeFilter;

  @JsonCreator
  public DynamoScanFilterSpec(@JsonProperty("hash") DynamoFilterSpec hashKeyFilter,
    @JsonProperty("range") DynamoFilterSpec rangeKeyFilter,
    @JsonProperty("attr") DynamoFilterSpec attributeFilter) {
    this.hashKeyFilter = hashKeyFilter;
    this.rangeKeyFilter = rangeKeyFilter;
    this.attributeFilter = attributeFilter;
  }

  public DynamoScanFilterSpec() {
  }

  public DynamoFilterSpec getHashKeyFilter() {
    return hashKeyFilter;
  }

  public void setHashKeyFilter(DynamoFilterSpec hashKeyFilter) {
    this.hashKeyFilter = hashKeyFilter;
  }

  public DynamoFilterSpec getRangeKeyFilter() {
    return rangeKeyFilter;
  }

  public void setRangeKeyFilter(DynamoFilterSpec rangeKeyFilter) {
    this.rangeKeyFilter = rangeKeyFilter;
  }

  public DynamoFilterSpec getAttributeFilter() {
    return attributeFilter;
  }

  public void setAttributeFilter(DynamoFilterSpec attributeFilter) {
    this.attributeFilter = attributeFilter;
  }
}
