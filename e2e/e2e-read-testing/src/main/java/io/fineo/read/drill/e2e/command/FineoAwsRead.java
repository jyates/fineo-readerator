package io.fineo.read.drill.e2e.command;

import com.beust.jcommander.ParametersDelegate;
import io.fineo.read.Driver;
import io.fineo.read.drill.e2e.options.DrillArguments;
import io.fineo.read.drill.e2e.options.ProfileAuthenticationOption;
import io.fineo.read.jdbc.FineoConnectionProperties;

import java.util.Properties;

public class FineoAwsRead extends Reader {

  @ParametersDelegate
  private final ProfileAuthenticationOption profile = new ProfileAuthenticationOption();

  public FineoAwsRead() {
    super( "io.fineo.read.Driver");
  }

  @Override
  public Properties loadProperties(DrillArguments opts) {
    Properties props = new Properties();
    props.put(FineoConnectionProperties.API_KEY.camelName(), opts.org.get());
    // load static .profile authentication
    props.put(FineoConnectionProperties.AUTHENTICATION.camelName(), "profile");
    props.put(FineoConnectionProperties.PROFILE_CREDENTIAL_NAME.camelName(), profile.name);
    return props;
  }

  @Override
  public String getJdbcConnection(String url) {
    return Driver.CONNECT_PREFIX;
  }
}
