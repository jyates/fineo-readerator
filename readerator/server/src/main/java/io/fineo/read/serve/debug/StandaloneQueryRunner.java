package io.fineo.read.serve.debug;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

/**
 * Helper class for debugging production connection. Runs a single SQL query against a drill
 * cluster.
 */
public class StandaloneQueryRunner {
  @Parameter(names = "--url",
             description = "URL of the target zookeeper.")
  public String url = "localhost:2181";

  @Parameter(names = "--query",
             description = "SQL query to run", required = true)
  public String query;

  public static void main(String[] args) throws Exception {
    StandaloneQueryRunner runner = new StandaloneQueryRunner();
    new JCommander(runner, args);

    // ensure we have the drill driver loaded
    Class.forName("org.apache.drill.jdbc.Driver");

    // run the query
    runner.run();
  }

  private void run() throws SQLException {
    // run the query
    try (Connection conn = DriverManager.getConnection(format("jdbc:drill:zk=%s", this.url));
         Statement stmt = conn.createStatement();
         ResultSet set = stmt.executeQuery(this.query)) {
      System.out.println("Running query: " + this.query);
      int row = 0;
      while (set.next()) {
        System.out.println((row++) + "---------------------------");
        ResultSetMetaData md = set.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
          System.out.println(i + ") [" + md.getColumnName(i) + "] " + set.getObject(i));
        }
      }
      System.out.println(" ** DONE **");
    }
  }
}
