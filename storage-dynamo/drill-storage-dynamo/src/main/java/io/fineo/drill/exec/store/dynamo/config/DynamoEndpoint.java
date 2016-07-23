package io.fineo.drill.exec.store.dynamo.config;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(DynamoEndpoint.NAME)
public class DynamoEndpoint {

  public static final String NAME = "aws";

  private final String regionOrUrl;

  public DynamoEndpoint(@JsonProperty("regionOrUrl") String regionOrUrl) {
    this.regionOrUrl = regionOrUrl;
  }

  @JsonIgnore
  public void configure(AmazonDynamoDBAsyncClient client) {
    // set the region or the url, depending on the format
    if (regionOrUrl.contains(":")) {
      client.setEndpoint(regionOrUrl);
    } else {
      client.setRegion(RegionUtils.getRegion(regionOrUrl));
    }
  }

  @JsonProperty("regionOrUrl")
  public String getRegionOrUrl() {
    return regionOrUrl;
  }
}
