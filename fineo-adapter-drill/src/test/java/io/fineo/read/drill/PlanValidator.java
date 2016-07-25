package io.fineo.read.drill;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class PlanValidator {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final String query;
  private Map<Integer, Consumer<Map<String, Object>>> validators = new HashMap<>();

  public PlanValidator(String query) {
    this.query = query;
  }

  public PlanValidator validateDynamoQuery(int graphIndex,
    Consumer<Map<String, Object>> validator) {
    validators.put(graphIndex, validator);
    return this;
  }

  public void validate(Connection conn) throws SQLException, IOException {
    String explain = explain(query);
    ResultSet plan = conn.createStatement().executeQuery(explain);
    assertTrue("After successful read, could not get the plan for query: " + explain, plan.next());
    String jsonPlan = plan.getString("json");
    Map<String, Object> jsonMap = MAPPER.readValue(jsonPlan, Map.class);
    List<Map<String, Object>> graph = (List<Map<String, Object>>) jsonMap.get("graph");
    for (Map.Entry<Integer, Consumer<Map<String, Object>>> validator : validators.entrySet()) {
      Map<String, Object> scan = graph.get(validator.getKey());
      validator.getValue().accept(scan);
    }
  }


  private String explain(String sql) {
    return "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITH IMPLEMENTATION FOR " + sql;
  }
}
