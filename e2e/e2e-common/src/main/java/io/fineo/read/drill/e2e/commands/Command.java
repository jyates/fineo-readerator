package io.fineo.read.drill.e2e.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.read.drill.e2e.options.DrillArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
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
    try (Connection conn = getConnection();
         ResultSet results = conn.createStatement().executeQuery(stmt);
         FileOutputStream os = new FileOutputStream(opts.outputFile);
         BufferedOutputStream bos = new BufferedOutputStream(os)) {
      List<Map<String, Object>> events = new ArrayList<>();
      while (results.next()) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < results.getMetaData().getColumnCount(); i++) {
          String col = results.getMetaData().getColumnName(i + 1);
          map.put(col, results.getObject(col));
        }
        events.add(map);
      }
      ObjectMapper mapper = new ObjectMapper();
      mapper.writeValue(bos, events);
    } finally {
      LOG.info("Done running query: {}", stmt);
    }
  }

  public abstract void run() throws Throwable;

  protected abstract Connection getConnection() throws Exception;
}
