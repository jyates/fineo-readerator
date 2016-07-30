package io.fineo.read.drill.e2e.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import io.fineo.drill.LocalDrillCluster;
import io.fineo.e2e.module.FakeAwsCredentialsModule;
import io.fineo.e2e.options.LocalSchemaStoreOptions;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.e2e.options.DrillArguments;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/**
 *
 */
public class LocalReadCommand {
  private static final Logger LOG = LoggerFactory.getLogger(LocalReadCommand.class);

  private static final Joiner AND = Joiner.on(" AND ");
  private final DrillArguments opts;
  private final LocalSchemaStoreOptions store;
  private LocalDrillCluster drill;

  public LocalReadCommand(DrillArguments opts, LocalSchemaStoreOptions storeOptions) {
    this.opts = opts;
    this.store = storeOptions;
  }

  public void run() throws Throwable {
    try {
      start();

      String from = format(" FROM fineo.%s.%s", opts.org.get(), opts.metric.get());
      String[] wheres = null;
      String where = wheres == null ? "" : " WHERE " + AND.join(wheres);
      String stmt = "SELECT *" + from + where + " ORDER BY `timestamp` ASC";

      try (Connection conn = drill.getConnection();
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

    } finally {
      if (this.drill != null) {
        drill.shutdown();
      }
    }
  }

  private void start() throws Throwable {
    LOG.info("Starting drill cluster...");
    drill = new LocalDrillCluster(1);
    drill.setup();

    LOG.info("Cluster started!");
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

  private void shutdown() {
    drill.shutdown();
  }
}
