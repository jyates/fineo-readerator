package io.fineo.drill.exec.store.dynamo.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Statically configured credentials. Its generally not advisable to use this - AWS will load
 * credentials onto machines that you can leverage with the 'profile' mode
 */
@JsonTypeName(StaticCredentialsConfig.NAME)
public class StaticCredentialsConfig {
  public static final String NAME = "static";

  private final String key;
  private final String secret;

  public StaticCredentialsConfig(@JsonProperty("key") String key,
    @JsonProperty("secret") String secret) {
    this.key = key;
    this.secret = secret;
  }

  public String getKey() {
    return key;
  }

  public String getSecret() {
    return secret;
  }
}
