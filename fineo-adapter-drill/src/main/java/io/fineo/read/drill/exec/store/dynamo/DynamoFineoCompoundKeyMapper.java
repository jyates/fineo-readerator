package io.fineo.read.drill.exec.store.dynamo;

import com.fasterxml.jackson.annotation.JacksonInject;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapper;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import io.fineo.schema.FineoStopWords;
import io.fineo.schema.store.AvroSchemaProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps the hash key into the org ID and metric ID
 */
public class DynamoFineoCompoundKeyMapper extends DynamoKeyMapper {
  protected DynamoFineoCompoundKeyMapper(@JacksonInject DynamoKeyMapperSpec spec) {
    super(spec);
  }

  @Override
  public Map<String, Object> mapHashKey(Object value) {
    String val = (String) value;
    int metricStart = val.indexOf(FineoStopWords.FIELD_PREFIX);
    String org = val.substring(0, metricStart);
    String metric = val.substring(metricStart);
    Map<String, Object> out = new HashMap<>();
    out.put(AvroSchemaProperties.ORG_ID_KEY, org);
    out.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metric);
    return out;
  }

  @Override
  public Map<String, Object> mapSortKey(Object value) {
    Map<String, Object> out = new HashMap<>();
    out.put(AvroSchemaProperties.TIMESTAMP_KEY, value);
    return out;
  }
}
