package io.fineo.read.drill.exec.store;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;

/**
 *
 */
public class FineoCommon {
  public static final List<String> REQUIRED_FIELDS =
    newArrayList(ORG_ID_KEY, ORG_METRIC_TYPE_KEY, TIMESTAMP_KEY);

  public static final String MAP_FIELD = "_fm";
}
