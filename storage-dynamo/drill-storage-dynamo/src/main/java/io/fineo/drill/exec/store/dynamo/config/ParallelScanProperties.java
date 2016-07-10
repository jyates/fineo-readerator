package io.fineo.drill.exec.store.dynamo.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Control the Scan properties. See:
 * <ol>
 *   <li>http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html</li>
 *   <li>http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#QueryAndScanParallelScan</li>
 * </ol>
 * For more information on tuning the scan segments.
 */
@JsonTypeName(ParallelScanProperties.NAME)
public class ParallelScanProperties {
  public static final String NAME = "parallel-scan";

  // maximum segments across all the possible endpoints. Will never be exceeded
  private int maxSegments;
  // maximum number of segments to run per endpoint. May be less than this if you have a low
  // number of max segments and lots of endpoints.
  private int segmentsPerEndpoint;
  // maximum number of rows to return from each scan request per endpoint.
  private int limit;

  @JsonProperty("max-segments")
  public void setMaxSegments(int maxSegments) {
    this.maxSegments = maxSegments;
  }

  @JsonProperty("max-segments")
  public int getMaxSegments() {
    return maxSegments;
  }

  @JsonProperty("rows-per-request")
  public void setLimit(int limit) {
    this.limit = limit;
  }

  @JsonProperty("rows-per-request")
  public int getLimit() {
    return limit;
  }

  @JsonProperty("segments-per-endpoint")
  public void setSegmentsPerEndpoint(int segmentsPerEndpoint) {
    this.segmentsPerEndpoint = segmentsPerEndpoint;
  }

  @JsonProperty("segments-per-endpoint")
  public int getSegmentsPerEndpoint() {
    return segmentsPerEndpoint;
  }
}
