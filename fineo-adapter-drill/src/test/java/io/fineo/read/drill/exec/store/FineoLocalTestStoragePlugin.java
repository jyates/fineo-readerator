package io.fineo.read.drill.exec.store;

import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.schema.FineoSchemaFactory;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.server.DrillbitContext;

// created by reflection to match the plugin configuration
public class FineoLocalTestStoragePlugin extends FineoStoragePlugin {
  public FineoLocalTestStoragePlugin(FineoLocalTestStoragePluginConfig configuration,
    DrillbitContext c, String name) throws ExecutionSetupException {
    super(configuration, c, name);
  }

  @Override
  protected FineoSchemaFactory getFactory(String name) {
    return new FineoLocalSchemaFactory(this, name);
  }
}
