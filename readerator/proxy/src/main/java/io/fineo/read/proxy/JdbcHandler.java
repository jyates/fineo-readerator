package io.fineo.read.proxy;

import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
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

import static fineo.client.com.google.common.base.Preconditions.checkNotNull;
import static io.fineo.read.jdbc.FineoConnectionProperties.API_KEY;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class JdbcHandler {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcHandler.class);
  public static final String APIKEY = "x-api-key";
  public static final String REQUEST = "request";
  private final String url;
  private final Properties defaults;

  public JdbcHandler(String jdbcUrl, Properties defaults) {
    this.url = checkNotNull(jdbcUrl, "No jdbc url provided!");
    this.defaults = defaults;
    LOG.info("Creating handler with url: {}", jdbcUrl);
  }

  @GET
  @Timed
  public List<Map<String, Object>> read(
    @QueryParam(REQUEST) String request,
    @HeaderParam(APIKEY) String apiKey) throws SQLException {
    Properties props = new Properties(defaults);
    props.put(API_KEY.camelName(),
      checkNotNull(apiKey, "Must include API Key as a query param %s!", APIKEY));
    checkNotNull(request, "Must provide query as a query parameter %s", REQUEST);
    try (Connection conn = DriverManager.getConnection(this.url, props);
         Statement statement = conn.createStatement();
         ResultSet results = statement.executeQuery(request)) {
      // pretty simple version to buffers all the results in memory... probably not the best, but
      // good enough for now...
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
