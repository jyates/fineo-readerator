package io.fineo.drill.exec.store.dynamo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.drill.exec.store.dynamo.config.ClientProperties;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;

/**
 * Fully define a scan of the table
 */
@JsonTypeName(DynamoScanSpec.NAME)
public class DynamoScanSpec {

  public static final String NAME = "dynamo-scan-spec";

  private final String tableName;
  private final ClientProperties client;
  private final ParallelScanProperties scan;

  @JsonCreator
  public DynamoScanSpec(@JsonProperty("tableName") String tableName,
    @JsonProperty("client") ClientProperties client,
    @JsonProperty("parallel-scan") ParallelScanProperties scan) {
    this.tableName = tableName;
    this.client = client;
    this.scan = scan;
  }

  public String getTableName() {
    return tableName;
  }

  public ClientProperties getClient() {
    return client;
  }

  public ParallelScanProperties getScan() {
    return scan;
  }
}
