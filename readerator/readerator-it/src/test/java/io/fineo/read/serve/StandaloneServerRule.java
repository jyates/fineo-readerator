package io.fineo.read.serve;

import net.hydromatic.scott.data.hsqldb.ScottHsqldb;
import org.junit.rules.ExternalResource;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class StandaloneServerRule extends ExternalResource {

  private final List<Runnable> befores;
  private final String org;
  private FineoServer server;
  private int port = -1;


  public StandaloneServerRule(String org, Runnable... befores) {
    this.befores = newArrayList(befores);
    this.org = org;
  }

  public StandaloneServerRule(String org, int port, Runnable... befores) {
    this.befores = newArrayList(befores);
    this.org = org;
    this.port = port;
  }

  @Override
  protected void before() throws Throwable {
    befores.forEach(Runnable::run);

    server = new FineoServer();
    server.setCatalogForTesting("PUBLIC");
    server.setOrgForTesting(org);
    if (port < 0) {
      port = new Random().nextInt(65535 - 49151);
      port += 49151;
    }
    Properties props = new Properties();
    props.put("user", ScottHsqldb.USER);
    props.put("password", ScottHsqldb.PASSWORD);
    server.setPropsForTesting(props);
    server.setPortForTesting(port);
    server.setDrillForTesting(ScottHsqldb.URI);
    server.start();
  }

  @Override
  protected void after() {
    server.stop();
  }

  public int port() {
    return this.port;
  }
}
