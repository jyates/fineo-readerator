package io.fineo.drill.exec.store.dynamo.config;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;

import java.util.Map;
import java.util.function.Function;

/**
 * Helper utility to generate the credentials from the credentials property map
 */
public class CredentialsUtil {

  public static final String CREDENTIALS_TYPE_KEY = "type";

  private enum Provider {
    DEFAULT(map -> new DefaultAWSCredentialsProviderChain()),
    PROFILE(map -> {
      String profileName = (String) map.get("name");
      return profileName != null ? new ProfileCredentialsProvider(profileName) :
             new ProfileCredentialsProvider();
    }),
    STATIC(map -> {
      Map<String, String> config = (Map<String, String>) map.get(StaticCredentialsConfig.NAME);
      String key = config.get("key");
      String value = config.get("secret");
      return new StaticCredentialsProvider(new BasicAWSCredentials(key, value));
    });

    private final Function<Map<String, Object>, AWSCredentialsProvider> func;

    Provider(Function<Map<String, Object>, AWSCredentialsProvider> func) {
      this.func = func;
    }

    public AWSCredentialsProvider create(Map<String, Object> map) {
      return func.apply(map);
    }
  }

  public static AWSCredentialsProvider getProvider(Map<String, Object> credentials) {
    String type = (String) credentials.get(CREDENTIALS_TYPE_KEY);
    if (type == null) {
      return Provider.DEFAULT.create(credentials);
    }

    Provider provider = Provider.valueOf(type.toUpperCase());
    if (provider == null) {
      throw new IllegalStateException("Credentials type: " + type + " not supported!");
    }

    return provider.create(credentials);
  }
}
