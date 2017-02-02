package io.fineo.read.proxy;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 * Simple proxy server for client HTTP-based JDBC requests.
 */
public class FineoJbcProxyServer extends Application<FineoProxyConfiguration> {
  @Override
  public void run(FineoProxyConfiguration config, Environment environment)
    throws Exception {
  }

  public static void main(String[] args) throws Exception {
    new FineoJbcProxyServer().run(args);
  }
}
