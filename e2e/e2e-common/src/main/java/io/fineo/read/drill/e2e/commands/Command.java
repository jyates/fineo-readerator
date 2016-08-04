package io.fineo.read.drill.e2e.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.read.drill.e2e.options.DrillArguments;
import io.fineo.e2e.module.FakeAwsCredentialsModule;
import io.fineo.e2e.options.LocalSchemaStoreOptions;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Command {
  private static final Logger LOG = LoggerFactory.getLogger(Command.class);

  protected final DrillArguments opts;
  protected final LocalSchemaStoreOptions store;

  public Command(DrillArguments opts, LocalSchemaStoreOptions storeOptions) {
    this.opts = opts;
    this.store = storeOptions;
  }

  protected void runQuery(String stmt) throws Exception {
    bootstrap();

    try (Connection conn = getConnection();
         ResultSet results = conn.createStatement().executeQuery(stmt);
         FileOutputStream os = new FileOutputStream(opts.outputFile);
         BufferedOutputStream bos = new BufferedOutputStream(os)) {
      List<Map<String, Object>> events = new ArrayList<>();
      while (results.next()) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < results.getMetaData().getColumnCount(); i++) {
          String col = results.getMetaData().getColumnName(i + 1);
          map.put(col, results.getObject(col));
        }
        events.add(map);
      }
      ObjectMapper mapper = new ObjectMapper();
      mapper.writeValue(bos, events);
    }
  }

  public abstract void run() throws Throwable;

  protected abstract Connection getConnection() throws Exception;

  private void bootstrap() throws IOException {
    LOG.info("Bootstrapping Fineo adapter...");

    // ensure the fineo plugin is setup
    BootstrapFineo bootstrap = new BootstrapFineo();
    BootstrapFineo.DrillConfigBuilder builder =
      bootstrap.builder()
               .withCredentials(new FakeAwsCredentialsModule().getCredentials())
               .withLocalDynamo("http://" + store.host + ":" + store.port)
               .withRepository(store.schemaTable)
               .withOrgs(opts.org.get());
    if (opts.dynamo.prefix != null) {
      builder
        .withDynamoTables(opts.dynamo.prefix, ".*")
        .withDynamoKeyMapper();
    }
    if (opts.input.get() != null) {
      builder.withLocalSource(new FsSourceTable("parquet", opts.input.get()));
    }

    // run the bootstrap
    if (!bootstrap.strap(builder)) {
      throw new RuntimeException("Bootstrap step failed!");
    }
    LOG.info("Fineo adapter bootstrapped!");
  }
}
