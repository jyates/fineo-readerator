package io.fineo.read.calcite;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

/**
 *
 */
public class DynamoSchemaFactory extends AbstractSchema{
  public Schema create(SchemaPlus parentSchema, String dynamoSchemaName,
    Map<String, Object> stringObjectMap) {
    return null;
  }
}
