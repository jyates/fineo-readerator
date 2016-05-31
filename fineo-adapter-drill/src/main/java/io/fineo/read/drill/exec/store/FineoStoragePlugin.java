package io.fineo.read.drill.exec.store;

import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

import java.io.IOException;

/**
 *
 */
public class FineoStoragePlugin extends AbstractStoragePlugin {

  private final FineoStoragePluginConfig config;
  private SchemaStore schemaStore;

  public FineoStoragePlugin(FineoStoragePluginConfig configuration, DrillbitContext context, String name){
    this.config = configuration;
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {

  }

  public SchemaStore getSchemaStore() {
    return schemaStore;
  }
}
