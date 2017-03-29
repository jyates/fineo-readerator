package io.fineo.read.proxy;

import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.util.Properties;

/**
 * Simple proxy server for client HTTP-based JDBC requests.
 */
public class FineoJbcProxyServer extends Application<FineoProxyConfiguration> {

  @Override
  public void initialize(Bootstrap<FineoProxyConfiguration> bootstrap) {
    // Enable variable substitution with environment variables
    bootstrap.setConfigurationSourceProvider(
      new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
        new EnvironmentVariableSubstitutor(true)
      )
    );
  }


  @Override
  public void run(FineoProxyConfiguration config, Environment environment)
    throws Exception {
    String url = config.getJdbcUrl();
    if(config.getJdbcPortFix() > 0){
      int ind = url.lastIndexOf(":");
      int port = Integer.valueOf(url.substring(ind+1));
      url = url.substring(0, ind)+(port -config.getJdbcPortFix());
    }
    environment.jersey().register(new JdbcHandler(url, new Properties()));
  }

  public static void main(String[] args) throws Exception {
    new FineoJbcProxyServer().run(args);
  }
}
