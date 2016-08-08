package io.fineo.read.jdbc;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import io.fineo.com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import io.fineo.com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import io.fineo.com.amazonaws.auth.ProfileCredentialsProvider;
import io.fineo.com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 */
public class AuthenticationUtil {

  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationUtil.class);
  private static final String AUTH_TYPE_SEPARATOR = "_OR_";

  public static void setupAuthentication(Properties info){
      // load all the places the credentials could be stored
      AWSCredentialsProviderChain chain = loadCredentialChain(info);
      String user = chain.getCredentials().getAWSAccessKeyId();
      String password = chain.getCredentials().getAWSSecretKey();
      info.setProperty(BuiltInConnectionProperty.AVATICA_USER.camelName(), user);
      info.setProperty(BuiltInConnectionProperty.AVATICA_PASSWORD.camelName(), password);
    }

  private static AWSCredentialsProviderChain loadCredentialChain(Properties info) {
    String authType = FineoConnectionProperties.AUTHENTICATION.wrap(info).getString();
    String[] types = authType.split(AUTH_TYPE_SEPARATOR);
    List<AWSCredentialsProvider> sources = new ArrayList<>();
    for (String type : types) {
      switch (type.toLowerCase()) {
        case "default":
          return new DefaultAWSCredentialsProviderChain();
        case "static":
          String key = FineoConnectionProperties.AWS_KEY.wrap(info).getString();
          String secret = FineoConnectionProperties.AWS_KEY.wrap(info).getString();
          sources.add(new StaticCredentialsProvider(new BasicAWSCredentials(key, secret)));
          break;
        case "system":
          sources.add(new SystemPropertiesCredentialsProvider());
          break;
        case "env":
          sources.add(new EnvironmentVariableCredentialsProvider());
          break;
        case "profile":
          sources.add(new ProfileCredentialsProvider(FineoConnectionProperties
            .PROFILE_CREDENTIAL_NAME.wrap(info).getString()));
          break;
        default:
          LOG.warn("No authentication provider of type {} supported!", type);
      }
    }

    return new AWSCredentialsProviderChain(sources.toArray(new AWSCredentialsProvider[0]));
  }
}
