package io.fineo.read.drill.e2e;

import com.google.common.base.Joiner;
import io.fineo.e2e.options.LocalSchemaStoreOptions;
import io.fineo.read.drill.StandaloneCluster;
import io.fineo.read.drill.e2e.commands.Command;
import io.fineo.read.drill.e2e.options.DrillArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

import static java.lang.String.format;

public class LocalReadCommand extends Command {
  private static final Logger LOG = LoggerFactory.getLogger(LocalReadCommand.class);

  private static final Joiner AND = Joiner.on(" AND ");
  private StandaloneCluster cluster;

  public LocalReadCommand(DrillArguments opts, LocalSchemaStoreOptions storeOptions) {
    super(opts, storeOptions);
  }

  @Override
  public void run() throws Throwable {
    try {
      start();

      String from = format(" FROM fineo.%s.%s", opts.org.get(), opts.metric.get());
      String[] wheres = null;
      String where = wheres == null ? "" : " WHERE " + AND.join(wheres);
      String stmt = "SELECT *" + from + where + " ORDER BY `timestamp` ASC";
      runQuery(stmt);
    } finally {
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
    bootstrap();
  }
}
