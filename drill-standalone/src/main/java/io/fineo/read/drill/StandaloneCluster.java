package io.fineo.read.drill;

import io.fineo.drill.LocalDrillCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * Wrapper around a {@link LocalDrillCluster} that allows us to run a local semi-distributed
 * Drill cluster.
 */
public class StandaloneCluster extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneCluster.class);
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
    this.drill = new LocalDrillCluster(1);
    drill.setup();

    Connection conn = drill.getConnection();
    FineoDrillStartupSetup setup = new FineoDrillStartupSetup(conn);
    setup.run();

    LOG.info("Cluster started!");
  }

  public Connection getConnection() throws Exception {
    return drill.getConnection();
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
