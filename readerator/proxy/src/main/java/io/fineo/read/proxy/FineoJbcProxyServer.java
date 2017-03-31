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
    environment.jersey().register(new JdbcHandler(config.getUrl(), new Properties()));
  }

  public static void main(String[] args) throws Exception {
    new FineoJbcProxyServer().run(args);
  }
}
