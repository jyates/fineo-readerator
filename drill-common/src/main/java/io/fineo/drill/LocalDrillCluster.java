package io.fineo.drill;

import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.jdbc.ConnectionFactory;
import org.apache.drill.jdbc.ConnectionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static java.lang.String.format;

public class LocalDrillCluster {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDrillCluster.class);
  private ZookeeperHelper zkHelper;
  private final int serverCount;
  private final SingleConnectionCachingFactory factory;
  private final Properties props = new Properties();
  private List<Drillbit> servers;

  {
    props.put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
    // turn off http to avoid conflicts between drill bits
//    props.put(ExecConstants.HTTP_ENABLE, "false");
    // pick a random ephemeral port for the bit/user
    int port = new Random().nextInt(65535 - 49152) + 49152;
    props.put("drill.exec.zk.connect", format("localhost:%s", ++port));
    // ++port because that matches how Drill looks for ports. Also user port before bit port
    props.put(ExecConstants.INITIAL_USER_PORT, Integer.toString(++port));
    props.put(ExecConstants.INITIAL_BIT_PORT, Integer.toString(++port));

  }

  public LocalDrillCluster(int serverCount) {
    this.serverCount = serverCount;
    this.factory = new SingleConnectionCachingFactory(new ConnectionFactory() {
      @Override
      public Connection getConnection(ConnectionInfo info) throws Exception {
        Class.forName("org.apache.drill.jdbc.Driver");
        return DriverManager.getConnection(info.getUrl(), info.getParamsAsProperties());
      }
    });
    zkHelper = new ZookeeperHelper(props, false);
  }

  public void setup() throws Throwable {
    zkHelper.startZookeeper(1);

    ImmutableList.Builder<Drillbit> servers = ImmutableList.builder();
    for (int i = 0; i < serverCount; i++) {
      servers.add(Drillbit.start(zkHelper.getConfig()));
    }
    this.servers = servers.build();
  }


  public void shutdown() {
    DrillMetrics.resetMetrics();

    if (servers != null) {
      for (Drillbit server : servers) {
        try {
          server.close();
        } catch (Exception e) {
          LOG.error("Error shutting down Drillbit", e);
        }
      }
    }

    zkHelper.stopZookeeper();

    try {
      factory.closeConnections();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public String getUrl() {
    String zkConnection = zkHelper.getConfig().getString("drill.exec.zk.connect");
    return format("jdbc:drill:zk=%s", zkConnection);
  }

  public Connection getConnection() throws Exception {
    return factory.getConnection(new ConnectionInfo(getUrl(), new Properties()));
  }
}
