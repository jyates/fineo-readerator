package io.fineo.read.http;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.mobileconnectors.apigateway.ApiClientFactory;
import io.fineo.read.jdbc.FineoConnectionProperties;
import io.fineo.read.jdbc.SystemPropertyPassThroughUtil;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.remote.AuthenticationType;
import org.apache.calcite.avatica.remote.AvaticaHttpClient;
import org.apache.calcite.avatica.remote.UsernamePasswordAuthenticateable;

import static io.fineo.read.jdbc.SystemPropertyPassThroughUtil.setInt;

/**
 * An Avatica client that writes/reads a Fineo AWS endpoint
 */
public class FineoAvaticaAwsHttpClient implements AvaticaHttpClient,
                                                  UsernamePasswordAuthenticateable {
  private final String url;
  private StaticCredentialsProvider credentials;
  private volatile Api client;

  public FineoAvaticaAwsHttpClient(String url, ConnectionConfig config) {
    this.url = url;
  }

  @Override
  public byte[] send(byte[] request) {
    ensureClient();
    return client.send(request).getBytes();
  }

  private void ensureClient() {
    synchronized (client) {
      if (client == null) {
        synchronized (client) {
          client = createClient();
        }
      }
    }
  }

  private Api createClient() {
    ApiClientFactory factory = new ApiClientFactory()
      .credentialsProvider(this.credentials)
      .clientConfiguration(getClientConfiguration())
      .apiKey(SystemPropertyPassThroughUtil.get(FineoConnectionProperties.API_KEY))
      .endpoint(url.toString());
    return factory.build(Api.class);
  }

  private ClientConfiguration getClientConfiguration() {
    ClientConfiguration client = new ClientConfiguration();
    setInt(FineoConnectionProperties.CLIENT_EXEC_TIMEOUT,
      prop -> client.withClientExecutionTimeout(prop));
    setInt(FineoConnectionProperties.CLIENT_MAX_CONNECTIONS,
      prop -> client.withMaxConnections(prop));
    setInt(FineoConnectionProperties.CLIENT_MAX_IDLE,
      prop -> client.withConnectionMaxIdleMillis(prop));
    setInt(FineoConnectionProperties.CLIENT_REQUEST_TIMEOUT,
      prop -> client.withRequestTimeout(prop));
    setInt(FineoConnectionProperties.CLIENT_INIT_TIMEOUT,
      prop -> client.withConnectionTimeout(prop));
    SystemPropertyPassThroughUtil.set(FineoConnectionProperties.CLIENT_TTL,
      prop -> {
        Long l = Long.parseLong(prop);
        if (l >= 0) {
          client.withConnectionTTL(l);
        }
      });
    return client;
  }

  @Override
  public void setUsernamePassword(AuthenticationType authType, String username, String password) {
    switch (authType) {
      case BASIC:
      case DIGEST:
        this.credentials =
          new StaticCredentialsProvider(new BasicAWSCredentials(username, password));
    }
  }
}
