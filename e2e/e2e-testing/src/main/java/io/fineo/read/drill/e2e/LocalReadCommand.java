package io.fineo.read.drill.e2e;

import com.google.common.base.Joiner;
import io.fineo.e2e.options.LocalSchemaStoreOptions;
import io.fineo.read.drill.StandaloneCluster;
import io.fineo.read.drill.e2e.commands.Command;
import io.fineo.read.drill.e2e.options.DrillArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public class LocalReadCommand extends Command {
  private static final Logger LOG = LoggerFactory.getLogger(LocalReadCommand.class);

  private StandaloneCluster cluster;

  private final LocalSchemaStoreOptions store;

  public LocalReadCommand(DrillArguments opts, LocalSchemaStoreOptions storeOptions) {
    super(opts);
    this.store = storeOptions;
  }

  @Override
  public void run() throws Throwable {
    try {
      start();
      runQuery(opts.sql.getQuery());
    } finally {
      LOG.info("Command failed = shutting down cluster!");
      if (this.cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Override
  protected Connection getConnection() throws Exception {
    return cluster.getConnection();
  }

  private void start() throws Throwable {
    LOG.info("Starting drill cluster...");
    cluster = new StandaloneCluster();
    cluster.runWithException();
    LOG.info("Cluster started!");
    opts.webPort = cluster.getWebPort();
    new Bootstrap(opts, store).run();
  }
}
