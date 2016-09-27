package io.fineo.read.drill.e2e;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import io.fineo.e2e.module.FakeAwsCredentialsModule;
import io.fineo.e2e.options.LocalSchemaStoreOptions;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.e2e.options.DrillArguments;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Bootstrap the fineo cluster with a configurable settings
 */
public class Bootstrap {
  private static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class);

  @ParametersDelegate
  protected final DrillArguments opts;
  @ParametersDelegate
  protected final LocalSchemaStoreOptions store;

  public Bootstrap() {
    this(new DrillArguments(), new LocalSchemaStoreOptions());
  }

  public Bootstrap(DrillArguments opts, LocalSchemaStoreOptions store) {
    this.opts = opts;
    this.store = store;
  }

  public void run() throws IOException {
    LOG.info("Bootstrapping Fineo adapter...");

    // ensure the fineo plugin is setup
    BootstrapFineo bootstrap = new BootstrapFineo(opts.webPort);
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

  public static void main(String[] args) throws IOException {
    Bootstrap bootstrap = new Bootstrap();
    JCommander jc = new JCommander(new Object[]{bootstrap});
    jc.parse(args);
    bootstrap.run();
  }
}
