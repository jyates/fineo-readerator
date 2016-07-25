package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.drill.exec.store.dynamo.DynamoPlanValidationUtils;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoQueryFilterSpec;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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

  public DynamoValidator validateDynamoQuery(int graphIndex) {
    DynamoValidator validator = new DynamoValidator();
    validators.put(graphIndex, validator);
    return validator;
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


  public class DynamoValidator implements Consumer<Map<String, Object>> {
    private List<String> columns;
    private String table;
    private DynamoReadFilterSpec scan;
    private List<DynamoReadFilterSpec> getOrQuery;

    public DynamoValidator withColumns(String... columns) {
      this.columns = Arrays.asList(columns).stream().map(BaseFineoTest::bt).collect(Collectors
        .toList());
      return this;
    }

    public DynamoValidator withTable(Table table) {
      this.table = table.getTableName();
      return this;
    }

    public DynamoValidator withScan(DynamoReadFilterSpec scan) {
      this.scan = scan;
      return this;
    }

    public DynamoValidator withGetOrQueries(DynamoReadFilterSpec ... getOrQuery) {
      this.getOrQuery = newArrayList(getOrQuery);
      return this;
    }

    public PlanValidator done() {
      return PlanValidator.this;
    }

    @Override
    public void accept(Map<String, Object> scan) {
      if (columns == null) {
        columns = newArrayList("`*`");
      }
      try {
        DynamoGroupScanSpec spec = DynamoPlanValidationUtils.validatePlan(scan, columns, this.scan,
          getOrQuery);
        DynamoTableDefinition tableDef = spec.getTable();
        assertEquals(table, tableDef.getName());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private String explain(String sql) {
    return "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITH IMPLEMENTATION FOR " + sql;
  }
}
