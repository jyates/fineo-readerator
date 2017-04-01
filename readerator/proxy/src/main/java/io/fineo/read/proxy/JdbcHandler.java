package io.fineo.read.proxy;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import io.fineo.read.proxy.exception.MissingParameterWebException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
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

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class JdbcHandler {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcHandler.class);
  public static final String APIKEY = "x-api-key";
  private final String url;
  private final Properties defaults;

  public JdbcHandler(String jdbcUrl, Properties defaults) {
    this.url = fineo.client.com.google.common.base.Preconditions
      .checkNotNull(jdbcUrl, "No jdbc url provided!");
    this.defaults = defaults;
    LOG.info("Creating handler with url: {}", jdbcUrl);
  }

  @POST
  @Timed
  public List<Map<String, Object>> read(
    String body,
    @HeaderParam(APIKEY) String apiKey) throws SQLException {
    checkNotNull(body, "body", "Must provide an SQL query");

    Properties props = new Properties(defaults);
    props.put(API_KEY.camelName(),
      checkNotNull(apiKey, "x-api-key", "Must be included in Header"));
    try (Connection conn = DriverManager.getConnection(this.url, props);
         Statement statement = conn.createStatement();
         ResultSet results = statement.executeQuery(body)) {
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

  private static <T> T checkNotNull(T obj, String param, String message, Object... injectInMessage)
    throws MissingParameterWebException {
    if (obj != null) {
      return obj;
    }
    if (injectInMessage != null && injectInMessage.length > 0) {
      message = String.format(message, injectInMessage);
    }

    throw new MissingParameterWebException(param, message);
  }

  private static final String QUOTE = "\"";
  private static final String SLASH_QUOTE = "\\\"";

  @VisibleForTesting
  static String decode(String query) throws UnsupportedEncodingException {
    String decoded = URLDecoder.decode(query, "UTF-8");
    int start = 0, end = 0;
    if (decoded.startsWith(QUOTE)) {
      start = 1;
    } else if (decoded.startsWith(SLASH_QUOTE)) {
      start = 2;
    }

    if (decoded.endsWith(SLASH_QUOTE)) {
      end = 2;
    } else if (decoded.endsWith(QUOTE)) {
      end = 1;
    }
    return decoded.substring(start, decoded.length() - end);
  }
}
