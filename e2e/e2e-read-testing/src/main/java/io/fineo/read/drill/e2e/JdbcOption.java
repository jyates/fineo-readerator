package io.fineo.read.drill.e2e;

import com.beust.jcommander.Parameter;

/**
 *
 */
public class JdbcOption {
  @Parameter(names = "--jdbc-connection",
             description = "JDBC connection string to Drill cluster. Generally of the form: "
                           + "jdbc:avatica:remote:serialization=protobuf;url=<host:post>;[[;"
                           + "option=value:port]]")
  public String jdbc;
}
