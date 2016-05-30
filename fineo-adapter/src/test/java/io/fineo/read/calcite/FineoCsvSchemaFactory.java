package io.fineo.read.calcite;

import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.adapter.csv.CsvSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Fineo schema that reads CSV files
 */
public class FineoCsvSchemaFactory extends FineoLocalSchemaFactory {

  public static final String CSV_SCHEMA_NAME = "csv-schema";

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    CsvSchemaFactory csvFactory = new CsvSchemaFactory();
    Map<String, Object> csvOperand = getNested(operand, CSV_SCHEMA_NAME);
    Schema csvSchema = csvFactory.create(parentSchema, DYNAMO_SCHEMA_NAME, csvOperand);
    parentSchema.add(DYNAMO_SCHEMA_NAME, csvSchema);

    SchemaStore store = createSchemaStore(operand);
    return new FineoSchema(parentSchema, store, csvSchema);
  }
}
