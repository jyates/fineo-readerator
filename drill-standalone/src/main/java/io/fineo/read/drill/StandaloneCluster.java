package io.fineo.read.drill;

import io.fineo.drill.LocalDrillCluster;
import io.fineo.read.drill.exec.store.ischema.FineoInfoSchemaUserFilters;
import org.apache.drill.exec.ExecConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Properties;

import static java.lang.String.format;

/**
 * Wrapper around a {@link LocalDrillCluster} that allows us to run a local semi-distributed
 * Drill cluster.
 */
public class StandaloneCluster extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneCluster.class);
  private static final int WEB_PORT = 8147;
  private LocalDrillCluster drill;

  public static void main(String[] args) throws InterruptedException {
    StandaloneCluster cluster = new StandaloneCluster();
    cluster.start();
    cluster.join();
  }

  @Override
  public void run() {
    try {
      runWithException();
      synchronized (this) {
        LOG.info("Waiting for calls to shutdown...");
        this.wait();
      }
    } catch (Throwable e) {
      throw new RuntimeException(e);
    } finally {
      LOG.info("Calling drill shutdown!");
      if (this.drill != null) {
        this.drill.shutdown();
      }
    }
  }

  public void runWithException() throws Throwable {
    Properties props = new Properties();
    // set some standard ports so we can connect with the defaults
    props.put(ExecConstants.ZK_CONNECTION, format("localhost:%s", 2181));
    props.put(ExecConstants.HTTP_PORT, Integer.toString(WEB_PORT));
    this.drill = new LocalDrillCluster(1, props);
    // make sure we override with our user filters
    drill.setup(FineoInfoSchemaUserFilters::overrideWithInfoSchemaFilters);

    Connection conn = drill.getConnection();
    FineoDrillStartupSetup setup = new FineoDrillStartupSetup(conn);
    setup.run();

    LOG.info("Cluster started! JDBC Url: " + drill.getUrl());
  }

  public Connection getConnection(Properties props) throws Exception {
    return drill.getUnmanagedConnection(props);
  }

  public int getWebPort() {
    int port = this.drill.getWebPort();
    assert port == WEB_PORT :
      format("Running web server on port [%] that doens't match configured [%s]", port, WEB_PORT);
    return port;
  }

  public void shutdown() {
    if (this.drill == null) {
      return;
    }
    this.drill.shutdown();
    this.drill = null;
    synchronized (this) {
      this.notifyAll();
    }
  }
}
