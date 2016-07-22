package io.fineo.drill.exec.store.dynamo;

import com.google.common.collect.ImmutableList;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DynamoSchemaFactory implements SchemaFactory {

  private final String name;
  private final DynamoStoragePlugin plugin;
  private final DynamoStoragePluginConfig conf;

  public DynamoSchemaFactory(String name, DynamoStoragePluginConfig conf,
    DynamoStoragePlugin plugin) {
    this.name = name;
    this.conf = conf;
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    DynamoSchema schema = new DynamoSchema(name);
    parent.add(name, schema);
  }

  private class DynamoSchema extends AbstractSchema {
    public DynamoSchema(String name) {
      super(ImmutableList.of(), name);
    }

    @Override
    public Set<String> getTableNames() {
      return StreamSupport.stream(plugin.getModel().listTables().pages().spliterator(), false)
                          .flatMap(page -> StreamSupport.stream(page.spliterator(), false))
                          .map(table -> table.getDescription().getTableName())
                          .collect(Collectors.toSet());
    }

    @Override
    public Table getTable(String name) {
      DynamoKeyMapperSpec keyMapper = null;
      Map<String, DynamoKeyMapperSpec> mappers = conf.getKeyMappers();
      if (mappers != null) {
        keyMapper = mappers.get(name);
      }

      return new DrillDynamoTable(plugin, name, keyMapper);
    }

    @Override
    public String getTypeName() {
      return DynamoStoragePlugin.NAME;
    }

    @Override
    public boolean isMutable() {
      return false;
    }
  }
}
