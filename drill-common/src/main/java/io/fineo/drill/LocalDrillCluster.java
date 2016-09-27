package io.fineo.drill;

import com.google.common.collect.ImmutableList;
import org.apache.drill.common.config.DrillConfig;
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
  private final Properties nonBootstrapProps = new Properties();
  private List<Drillbit> servers;
  private int webPort = -1;

  {
    props.put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
    // pick a random ephemeral port for all the resources to start at
    int port = new Random().nextInt(65535 - 49152) + 49152;
    webPort = ++port;
    props.put(ExecConstants.HTTP_PORT, Integer.toString(webPort));
    props.put(ExecConstants.HTTP_ENABLE, "true");
    props.put("drill.exec.zk.connect", format("localhost:%s", ++port));
    // ++port because that matches how Drill looks for ports. Also user port before bit port
    props.put(ExecConstants.INITIAL_USER_PORT, Integer.toString(++port));
    props.put(ExecConstants.INITIAL_BIT_PORT, Integer.toString(++port));

    nonBootstrapProps.putAll(props);
    // turn off http to avoid conflicts between drill bits
    nonBootstrapProps.put(ExecConstants.HTTP_ENABLE, "false");
  }

  public LocalDrillCluster(int serverCount) {
    this(serverCount, null);
  }

  public LocalDrillCluster(int serverCount, Properties overrides) {
    this.serverCount = serverCount;
    this.factory = new SingleConnectionCachingFactory(new ConnectionFactory() {
      @Override
      public Connection getConnection(ConnectionInfo info) throws Exception {
        Class.forName("org.apache.drill.jdbc.Driver");
        return DriverManager.getConnection(info.getUrl(), info.getParamsAsProperties());
      }
    });
    // override properties
    if (overrides != null) {
      props.putAll(overrides);
      nonBootstrapProps.putAll(overrides);
    }

    zkHelper = new ZookeeperHelper(props, false);
  }

  public void setup() throws Throwable {
    zkHelper.startZookeeper(1);

    ImmutableList.Builder<Drillbit> servers = ImmutableList.builder();
    for (int i = 0; i < serverCount; i++) {
      DrillConfig config = i == 0 ? zkHelper.getConfig() : DrillConfig.create(nonBootstrapProps);
      servers.add(Drillbit.start(config));
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

  public int getWebPort() {
    return this.webPort;
  }
}
