package io.fineo.read.drill.e2e.options;

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
    StringBuffer sb = new StringBuffer("url=https://");
    sb.append(host);
    if(port != null){
      sb.append(":");
      sb.append(port);
    }
    return sb.toString();
  }
}
