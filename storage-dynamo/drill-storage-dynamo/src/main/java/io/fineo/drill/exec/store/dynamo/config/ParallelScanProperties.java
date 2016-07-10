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
  private final int maxSegments;
  // maximum number of segments to run per endpoint. May be less than this if you have a low
  // number of max segments and lots of endpoints.
  private final int segmentsPerEndpoint;
  // maximum number of rows to return from each scan request per endpoint.
  private final int limit;

  public ParallelScanProperties(@JsonProperty("max-segments") int maxSegments,
    @JsonProperty("segments-per-endpoint") int segmentsPerEndpoint,
    @JsonProperty("rows-per-request") int limit) {
    this.maxSegments = maxSegments;
    this.segmentsPerEndpoint = segmentsPerEndpoint;
    this.limit = limit;
  }

  @JsonProperty
  public int getMaxSegments() {
    return maxSegments;
  }

  @JsonProperty
  public int getLimit() {
    return limit;
  }

  @JsonProperty
  public int getSegmentsPerEndpoint() {
    return segmentsPerEndpoint;
  }
}
