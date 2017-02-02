package io.fineo.read.proxy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.fineo.read.jdbc.FineoConnectionProperties.API_KEY;

/**
 *
 */
public class JdbcHandler {

  public List<Map<String, Object>> read(String request, String apiKey) throws SQLException {
    Properties props = new Properties();
    props.put(API_KEY, apiKey);
    try (Connection conn = DriverManager.getConnection(request, props);
         Statement statement = conn.createStatement();
         ResultSet results = statement.executeQuery(request)) {
      List<Map<String, Object>> out = new ArrayList<>();
      while (results.next()) {
        Map<String, Object> row = new HashMap<>();
        out.add(row);
        ResultSetMetaData meta = results.getMetaData();
        int columns = meta.getColumnCount();
        for (int i = 1; i <= columns; i++) {
          String name = meta.getColumnName(i);
          Object value = results.getObject(i);
          row.put(name, value);
        }
      }

      return out;
    }
  }
}
