package io.fineo.read.drill.exec.store;

import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class FineoStoragePlugin extends AbstractStoragePlugin {

  private static final Logger LOG = LoggerFactory.getLogger(FineoStoragePlugin.class);

  private final FineoStoragePluginConfig config;
  private SchemaStore schemaStore;

  public FineoStoragePlugin(FineoStoragePluginConfig configuration, DrillbitContext context, String name){
    LOG.info("Jesse - Creating storage plugin!");
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
