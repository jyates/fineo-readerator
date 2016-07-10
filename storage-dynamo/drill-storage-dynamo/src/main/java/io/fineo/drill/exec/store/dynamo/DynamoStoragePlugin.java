package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

import java.io.IOException;
import java.util.List;

public class DynamoStoragePlugin extends AbstractStoragePlugin {

  public static final String NAME = "dynamo";

  private final DynamoStoragePluginConfig config;
  private final String name;
  private final DynamoSchemaFactory factory;
  private final DrillbitContext context;
  private AmazonDynamoDBAsyncClient client;
  private DynamoDB model;

  public DynamoStoragePlugin(DynamoStoragePluginConfig conf, DrillbitContext c,
    String name) {
    this.context = c;
    this.config = conf;
    this.name = name;
    this.factory = new DynamoSchemaFactory(name, conf, this);
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    factory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
    List<SchemaPath> columns) throws IOException {
    DynamoScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new
      TypeReference<DynamoScanSpec>() {
      });
    return new DynamoGroupScan(this, scanSpec, columns);
  }

  public DrillbitContext getContext() {
    return context;
  }

  @Override
  public void close() throws Exception {
    this.model.shutdown();
    this.client.shutdown();
  }

  public DynamoDB getModel() {
    ensureModel();
    return model;
  }

  private void ensureModel() {
    if (this.model == null) {
      this.client = new AmazonDynamoDBAsyncClient(config.inflateCredentials());
      config.getEndpoint().configure(client);
      this.model = new DynamoDB(client);
    }
  }
}
