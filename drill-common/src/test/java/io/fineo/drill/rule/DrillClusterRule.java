package io.fineo.drill.rule;

import io.fineo.drill.LocalDrillCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.common.config.DrillConfig;
import org.junit.rules.ExternalResource;

import java.sql.Connection;
import java.util.Properties;
import java.util.function.Function;

/**
 * Create and destroy a drill cluster as a junit rule
 */
public class DrillClusterRule extends ExternalResource {

  private static final Log LOG = LogFactory.getLog(DrillClusterRule.class);
  private final LocalDrillCluster drill;
  private Function<DrillConfig, DrillConfig> hook = Function.identity();
  private boolean running = false;

  public DrillClusterRule(int serverCount) {
    drill = new LocalDrillCluster(serverCount);
  }

  public synchronized void overrideConfigHook(Function<DrillConfig, DrillConfig> hook)
    throws Throwable {
    this.hook = hook;
    // every time we update the config, reboot the cluster
    if (this.running) {
      LOG.info("Rebooting cluster because of a config hook change!");
      this.after();
      this.before();
    }
  }

  @Override
  protected synchronized void before() throws Throwable {
    drill.setup(hook);
    running = true;
  }

  @Override
  protected synchronized void after() {
    drill.shutdown();
  }

  public Connection getConnection() throws Exception {
    return drill.getConnection();
  }

  public Connection getUnmanagedConnection(Properties props) throws Exception {
    return drill.getUnmanagedConnection(props);
  }

  public int getWebPort() {
    return drill.getWebPort();
  }
}
