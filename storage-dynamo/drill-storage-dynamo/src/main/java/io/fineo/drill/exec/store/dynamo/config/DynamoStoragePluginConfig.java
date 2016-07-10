package io.fineo.drill.exec.store.dynamo.config;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import org.apache.drill.common.logical.StoragePluginConfig;

import java.util.Map;

@JsonTypeName(DynamoStoragePluginConfig.NAME)
public class DynamoStoragePluginConfig extends StoragePluginConfig {
  public static final String NAME = "dynamo";

  private final AWSCredentialsProvider inflatedCredentials;
  private DynamoEndpoint endpoint;
  private final ClientProperties client;
  private final ParallelScanProperties scan;
  private Map<String, Object> credentials;

  @JsonCreator
  public DynamoStoragePluginConfig(
    @JsonProperty("credentials") Map<String, Object> credentials,
    @JsonProperty(DynamoEndpoint.NAME) DynamoEndpoint endpoint,
    @JsonProperty(ClientProperties.NAME) ClientProperties client,
    @JsonProperty(ParallelScanProperties.NAME) ParallelScanProperties scan) {
    this.credentials = credentials;
    this.inflatedCredentials = CredentialsUtil.getProvider(credentials);
    this.endpoint = endpoint;
    this.client = client;
    this.scan = scan;
  }

  @JsonIgnore
  public AWSCredentialsProvider inflateCredentials() {
    return inflatedCredentials;
  }

  @SuppressWarnings("unused")
  public Map<String, Object> getCredentials() {
    return credentials;
  }

  public DynamoEndpoint getEndpoint() {
    return endpoint;
  }

  public ClientProperties getClient() {
    return client;
  }

  public ParallelScanProperties getScan() {
    return scan;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof DynamoStoragePluginConfig))
      return false;

    DynamoStoragePluginConfig that = (DynamoStoragePluginConfig) o;
    if (!inflateCredentials().equals(that.inflateCredentials())) {
      return false;
    }
    return getEndpoint().equals(that.getEndpoint());
  }

  @Override
  public int hashCode() {
    int result = inflateCredentials().hashCode();
    result = 31 * result + getEndpoint().hashCode();
    return result;
  }

  @VisibleForTesting
  @JsonIgnore
  public void setEndpointForTesting(DynamoEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  @VisibleForTesting
  @JsonIgnore
  public void setCredentialsForTesting(Map<String, Object> credentials) {
    this.credentials = credentials;
  }
}
