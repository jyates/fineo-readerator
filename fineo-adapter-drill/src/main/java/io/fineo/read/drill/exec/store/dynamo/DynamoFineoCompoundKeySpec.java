package io.fineo.read.drill.exec.store.dynamo;

import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import io.fineo.schema.store.AvroSchemaProperties;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Helper spec for the standard fineo compound key specification
 */
public class DynamoFineoCompoundKeySpec extends DynamoKeyMapperSpec {
  public DynamoFineoCompoundKeySpec() {
    super(asList(AvroSchemaProperties.ORG_ID_KEY, AvroSchemaProperties.ORG_METRIC_TYPE_KEY,
      AvroSchemaProperties.TIMESTAMP_KEY), asList("S", "S", "N"),
      args());
  }

  private static Map<String, Object> args() {
    Map<String, Object> args = new HashMap<>();
    args.put("@class", DynamoFineoCompoundKeyMapper.class);
    return args;
  }
}
