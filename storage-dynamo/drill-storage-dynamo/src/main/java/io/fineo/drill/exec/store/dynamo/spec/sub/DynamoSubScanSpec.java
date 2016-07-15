package io.fineo.drill.exec.store.dynamo.spec.sub;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import org.apache.drill.common.expression.SchemaPath;

import java.util.List;


@JsonTypeName("dynamo-sub-scan-spec")
public class DynamoSubScanSpec extends DynamoSubReadSpec {
  private final int totalSegments;
  private final int segmentId;

  public DynamoSubScanSpec(@JsonProperty("filter")DynamoReadFilterSpec filter,
    @JsonProperty("segments") int totalSegments,
    @JsonProperty("segment-id") int segmentId,
    @JsonProperty("columns") List<SchemaPath> columns) {
    super(filter, columns);
    this.totalSegments = totalSegments;
    this.segmentId = segmentId;
  }

  public int getTotalSegments() {
    return totalSegments;
  }

  public int getSegmentId() {
    return segmentId;
  }
}
