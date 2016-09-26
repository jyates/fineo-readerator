package io.fineo.read.drill.e2e.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.read.drill.e2e.DelegateConnection;
import io.fineo.read.drill.e2e.options.DrillArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Command {
  private static final Logger LOG = LoggerFactory.getLogger(Command.class);

  protected final DrillArguments opts;

  public Command(DrillArguments opts) {
    this.opts = opts;
  }

  protected void runQuery(String stmt) throws Exception {
    int i = 0;
    boolean success = false;
    // try 10 times
    while (i++ < 10) {
      try (Connection conn = connection();
           FileOutputStream os = new FileOutputStream(opts.outputFile);
           BufferedOutputStream bos = new BufferedOutputStream(os)) {
        List<Map<String, Object>> events = tryRequest(conn, stmt);
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(bos, events);
        success = true;
        return;
      } catch (RuntimeException e) {
        if (!e.getMessage().contains("Failure setting up ZK for client")) {
          throw e;
        } else {
          LOG.error("Failed to connect!", e);
          LOG.info(" ---- Sleeping to await connection...");
          Thread.sleep(5000);
        }
      } finally {
        LOG.info("{}, Done running query: {}", stmt, success ? "[SUCCESS]" : "[FAILURE]");
      }
    }

    LOG.info("And finished with query method - **** FAILED ****");
    System.exit(1);
  }

  private List<Map<String, Object>> tryRequest(Connection conn, String stmt) throws Exception {
    try (ResultSet results = conn.createStatement().executeQuery(stmt)) {
      List<Map<String, Object>> events = new ArrayList<>();
      while (results.next()) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < results.getMetaData().getColumnCount(); i++) {
          String col = results.getMetaData().getColumnName(i + 1);
          map.put(col, results.getObject(col));
        }
        events.add(map);
      }
      return events;
    }
  }

  private Connection connection() throws Exception {
    return new DelegateConnection(getConnection()) {
      @Override
      public void close() throws SQLException {
        LOG.info("Closing connection...");
        super.close();
      }
    };
  }

  public abstract void run() throws Throwable;

  protected abstract Connection getConnection() throws Exception;
}
