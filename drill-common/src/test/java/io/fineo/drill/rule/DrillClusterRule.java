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

  public DrillClusterRule(int serverCount) {
    drill = new LocalDrillCluster(serverCount);
  }

  public void overrideConfigHook(Function<DrillConfig, DrillConfig> hook){
    this.hook = hook;
  }

  @Override
  protected void before() throws Throwable {
    drill.setup(hook);
  }

  @Override
  protected void after() {
    drill.shutdown();
  }

  public Connection getConnection() throws Exception {
    return drill.getConnection();
  }

  public Connection getUnmanagedConnection(Properties props) throws Exception{
    return drill.getUnmanagedConnection(props);
  }

  public int getWebPort(){
    return drill.getWebPort();
  }
}
