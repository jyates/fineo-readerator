package io.fineo.read.drill.e2e;

import com.beust.jcommander.Parameter;

import static java.lang.String.format;

/**
 *
 */
public class JdbcOption {
  @Parameter(names = "--jdbc-host", description = "JDBC host string to Avatica server")
  public String host;

  @Parameter(names = "--jdbc-port", description = "JDBC port string to Avatica server")
  public String port;

  public String getUrl(){
    return format("jdbc:avatica:remote:serialization=protobuf;url=http://%s:%s", host, port);
  }
}
