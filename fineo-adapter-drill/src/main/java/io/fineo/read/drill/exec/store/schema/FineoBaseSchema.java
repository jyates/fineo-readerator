package io.fineo.read.drill.exec.store.schema;


import org.apache.drill.exec.store.AbstractSchema;

import java.util.List;

/**
 * Base Schema for Fineo sub-schemas
 */
public abstract class FineoBaseSchema extends AbstractSchema {
  public FineoBaseSchema(List<String> parentSchemaPath, String name) {
    super(parentSchemaPath, name);
  }

  @Override
  public String getTypeName() {
    return "FINEO";
  }

  @Override
  public boolean isMutable() {
    return false;
  }
}
