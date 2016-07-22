package io.fineo.read.drill.exec.store.dynamo;

import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import io.fineo.schema.avro.AvroSchemaEncoder;

import java.util.HashMap;
import java.util.Map;

import static org.apache.calcite.util.ImmutableNullableList.of;

/**
 * Helper spec for the standard fineo compound key specification
 */
public class DynamoFineoCompoundKeySpec extends DynamoKeyMapperSpec {
  public DynamoFineoCompoundKeySpec() {
    super(of(AvroSchemaEncoder.ORG_ID_KEY, AvroSchemaEncoder.ORG_METRIC_TYPE_KEY), of("S", "S"),
      args());
  }

  private static Map<String, Object> args() {
    Map<String, Object> args = new HashMap<>();
    args.put("@class", DynamoFineoCompoundKeyMapper.class);
    return args;
  }
}
