package io.fineo.read.drill.exec.store;

import io.fineo.read.drill.exec.store.plugin.FineoStoragePluginConfig;
import io.fineo.read.drill.exec.store.schema.FineoSchema;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;

import java.io.IOException;

/**
 * Fineo schema that reads JSON files
 */
public class FineoJsonSchemaFactory extends FineoLocalSchemaFactory {

  public FineoJsonSchemaFactory(FineoLocalTestStoragePlugin fineoStoragePlugin, String name) {
    super(fineoStoragePlugin, name);
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    FineoLocalTestStoragePluginConfig conf =
      (FineoLocalTestStoragePluginConfig) this.plugin.getConfig();
    SchemaStore store = createSchemaStore(conf);

    // add the ability to read json files here...

    parent.add(FineoStoragePluginConfig.NAME,
      new FineoSchema(parent, this.name, this.plugin, store, null));
  }
}
