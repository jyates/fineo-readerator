package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoGetFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoQueryFilterSpec;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec.create;
import static org.apache.commons.lang3.tuple.ImmutablePair.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestDynamoFilterPushdown extends BaseDynamoTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Table with just a hash key has the hash key fully specified, which should cause a single Get
   * request
   */
  @Test
  public void testPrimaryKeyFilterSpecifiedHashKey() throws Exception {
    Item item = item();
    item.with(COL1, "v1");
    Item i2 = new Item();
    i2.with(PK, "2");
    i2.with(COL1, "pk");
    Table table = createTableWithItems(item, i2);
    String select = selectStarWithPK("2", "t", table);
    verify(runAndReadResults(select), i2);
    validatePlanWithGets(select, create("equal", PK, "2"));
  }

  @Test
  public void testPrimaryAndSortKeySpecification() throws Exception {
    String pk = "pk", sort = "sort";
    Table table = createHashAndSortTable(pk, sort);
    Item item = new Item();
    item.with(pk, "p1");
    item.with(sort, "s1");
    item.with(COL1, "1");
    table.putItem(item);

    Item item2 = new Item();
    item2.with(pk, "p1");
    item2.with(sort, "s0");
    item2.with(COL1, "2");
    table.putItem(item2);
    // should create a get
    String query = selectStarWithPK("p1", "t", table) + " AND sort = 's1'";
    verify(runAndReadResults(query), item);
    validatePlanWithGets(query, pkEquals("p1").and(create("equal", "sort", "s1")));
    // should create a query
    query = selectStarWithPK("p1", "t", table) + " AND sort >= 's1'";
    verify(runAndReadResults(query), item);
    validatePlanWithQueries(query, of(pkEquals("p1").and(gte("sort", "s1")), null));
  }

  private DynamoFilterSpec gte(String key, Object val) {
    return create("greater_than_or_equal_to", key, val);
  }

  private DynamoFilterSpec pkEquals(Object eq) {
    return equals(PK, eq);
  }

  private DynamoFilterSpec equals(String key, Object value) {
    return create("equal", key, value);
  }

  @Test
  public void testPrimaryKeyAndAttributeFilter() throws Exception {
    Item item = item();
    item.with(COL1, "1");
    Table t = createTableWithItems(item);
    String query = selectStarWithPK("pk", "t", t) + " AND t." + COL1 + " = '1'";
    verify(runAndReadResults(query), item);
    validatePlanWithQueries(query, of(pkEquals("pk"), equals(COL1, "1")));

    // number column
    item = new Item();
    item.with(PK, "pk2");
    item.with(COL1, 1);
    t.putItem(item);
    query = selectStarWithPK("pk2", "t", t) + " AND t." + COL1 + " = 1";
    verify(runAndReadResults(query), item);
    validatePlanWithQueries(query, of(pkEquals("pk2"), equals(COL1, 1)));
  }

  @Test
  public void testWhereColumnEqualsNull() throws Exception {
    Item item = item();
    item.with(COL1, null);
    Table table = createTableWithItems(item);

    String query =
      selectStarWithPK("pk", "t", table) + " AND t." + COL1 + " = cast(null as varchar)";
    verify(runAndReadResults(query), item);
    ImmutablePair<DynamoFilterSpec, DynamoFilterSpec> spec = of(pkEquals("pk"), equals(COL1, null));
    validatePlanWithQueries(query, spec);
    // we return nulls are varchar b/c we can cast anything from varchar. Make sure that a
    // boolean null cast also works
    query = selectStarWithPK("pk", "t", table) + " AND t." + COL1 + " = cast(null as boolean)";
    verify(runAndReadResults(query), item);
    validatePlanWithQueries(query, spec);
  }

  /**
   * Similar to above, but we check for the non-existance of a column
   */
  @Test
  public void testWhereNoColumnValueIsNull() throws Exception {
    Item item = item();
    Table table = createTableWithItems(item);
    String query =
      selectStarWithPK("pk", "t", table) + " AND t." + COL1 + " = cast(null as varchar)";
    assertEquals("Should not have found a row when checking for = null and column not set!",
      0, runAndReadResults(query).size());
    ImmutablePair<DynamoFilterSpec, DynamoFilterSpec> spec = of(pkEquals("pk"), equals(COL1, null));
    validatePlanWithQueries(query, spec);
    // see above for why trying a different type
    query = selectStarWithPK("pk", "t", table) + " AND t." + COL1 + " = cast(null as BOOLEAN)";
    assertEquals("Should not have found a row when checking for = null and column not set!",
      0, runAndReadResults(query).size());
    validatePlanWithQueries(query, spec);
    query = selectStarWithPK("pk", "t", table) + " AND t." + COL1 + " IS NULL";
    verify(runAndReadResults(query), item);
    validatePlanWithQueries(query, of(spec.getLeft(), create("isNull", COL1)));
  }

  @Test
  public void testSimpleScan() throws Exception {
    Item item = item();
    item.with(COL1, 1);
    Table table = createTableWithItems(item);
    String select = "SELECT *" + from(table) + "t WHERE t." + COL1 + " = 1";
    verify(runAndReadResults(select), item);
    DynamoFilterSpec spec = equals(COL1, 1);
    validatePlanWithScan(select, spec);

    Item item2 = new Item();
    item2.with(PK, "pk2");
    item2.with(COL1, 2);
    table.putItem(item2);
    verify(runAndReadResults(select), item);
    // plan doesn't change as the table gets larger
    validatePlanWithScan(select, spec);
  }

  @Test
  public void testTwoPointGets() throws Exception {
    Item item = item();
    item.with(COL1, "1");
    Item i2 = new Item();
    i2.with(PK, "pk2");
    i2.with(COL1, "2");
    Table table = createTableWithItems(item, i2);
    String query = "SELECT *" + from(table) + "t WHERE " +
                   "t." + PK + " = 'pk' OR " +
                   "t." + PK + " = 'pk2'" +
                   "ORDER BY t." + PK + " ASC";
    verify(runAndReadResults(query), item, i2);
    validatePlanWithGets(query, pkEquals("pk2"), pkEquals("pk"));
  }

  @Test
  public void testPointGetWithRetainedFilterCausesQuery() throws Exception {
    Item item = item();
    item.with(COL1, "1");
    Item i2 = new Item();
    i2.with(PK, "pk2");
    i2.with(COL1, "2");
    Table table = createTableWithItems(item, i2);
    String query = "SELECT *" + from(table) + "t WHERE " +
                   "t." + PK + " = 'pk2' AND " +
                   "t." + COL1 + " = '2'";
    verify(runAndReadResults(query), i2);
    validatePlanWithQueries(query, of(pkEquals("pk2"), equals(COL1, "2")));
  }

  @Test
  public void testGetAndQuery() throws Exception {
    Item item = item();
    item.with(COL1, "1");
    Item i2 = new Item();
    i2.with(PK, "pk2");
    i2.with(COL1, 2);
    Table table = createTableWithItems(item, i2);
    String query = "SELECT *" + from(table) + "t WHERE " +
                   "t." + PK + " = 'pk' OR " +
                   "t." + PK + " = 'pk2' AND t." + COL1 + " >= 2" +
                   "ORDER BY t." + PK + " ASC";
    verify(runAndReadResults(query), item, i2);
    validatePlan(query, null, null,
      newArrayList(new DynamoQueryFilterSpec(pkEquals("pk2"), gte(COL1, 2)),
        new DynamoGetFilterSpec(pkEquals("pk"))));
  }

  @Test
  public void testQueryOrQuery() throws Exception {
    Item item = item();
    item.with(COL1, 1);
    Item i2 = new Item();
    i2.with(PK, "pk2");
    i2.with(COL1, 2);
    Table table = createTableWithItems(item, i2);
    String query = "SELECT *" + from(table) + "t WHERE " +
                   "t." + PK + " = 'pk' AND t." + COL1 + " = 1" +
                   " OR " +
                   "t." + PK + " = 'pk2' AND t." + COL1 + " >= 2" +
                   "ORDER BY t." + PK + " ASC";
    verify(runAndReadResults(query), item, i2);
    validatePlanWithQueries(query,
      of(pkEquals("pk2"), gte(COL1, 2)),
      of(pkEquals("pk"), equals(COL1, 1)));
  }

  @Test
  public void testQueryAndQueryForcesScan() throws Exception {
    Item item = item();
    item.with(COL1, 1);
    Item i2 = new Item();
    i2.with(PK, "pk2");
    i2.with(COL1, 2);
    Table table = createTableWithItems(item, i2);
    String query = "SELECT *" + from(table) + "t WHERE " +
                   "t." + PK + " = 'pk' AND t." + COL1 + " = 1" +
                   " AND " +
                   "t." + PK + " = 'pk2' AND t." + COL1 + " >= 2" +
                   "ORDER BY t." + PK + " ASC";
    verify(runAndReadResults(query));
    validatePlanWithScan(query,
      pkEquals("pk").and(equals(COL1, 1)).and(pkEquals("pk2")).and(gte(COL1, 2)));
  }

  @Test
  public void testQueryOrAttributeForcesScan() throws Exception {
    Item item = item();
    item.with(COL1, 1);
    Item i2 = new Item();
    i2.with(PK, "pk2");
    i2.with(COL1, 2);
    Table table = createTableWithItems(item, i2);
    String query = "SELECT *" + from(table) + "t WHERE " +
                   "(t." + PK + " = 'pk' AND t." + COL1 + " = 1)" +
                   " OR " +
                   "t." + COL1 + " >= 2" +
                   "ORDER BY t." + PK + " ASC";
    verify(runAndReadResults(query), item, i2);
    validatePlanWithScan(query, pkEquals("pk").and(equals(COL1, 1)).or(gte(COL1, 2)));
  }

  @Test
  public void testMultiRangeQuery() throws Exception {
    Table table = createHashAndSortTable(PK, COL1);
    Item item = item();
    item.with(COL1, "1");
    table.putItem(item);
    Item i2 = item();
    i2.with(COL1, "2");
    table.putItem(i2);
    String query = "SELECT *" + from(table) + "t WHERE " +
                   "t." + PK + " = 'pk'" + " AND (" +
                   "t." + COL1 + " = '1'" +
                   " AND " +
                   "t." + COL1 + " >= '2')";
    verify(runAndReadResults(query));
    validatePlanWithQueries(query, of(pkEquals("pk").and(equals(COL1, "1")), null),
      of(pkEquals("pk").and(gte(COL1, "2")), null));
//    verify(runAndReadResults("SELECT *" + from(table) + "t WHERE " +
//                             "t." + PK + " = 'pk'" + " AND (" +
//                             "t." + COL1 + " = '1'" +
//                             " OR " +
//                             "t." + COL1 + " >= '2')"), item, i2);
  }

  @Test
  public void testBetweenScan() throws Exception {
    Item item = item();
    item.with(COL1, "1");
    Table table = createTableWithItems(item);
    String query = "SELECT *" + from(table) + "t WHERE " +
                   "t." + COL1 + " BETWEEN '1' AND '2'";
    verify(runAndReadResults(query), item);
    validatePlanWithScan(query, create("between", COL1, "1", "2"));
  }

  @Test
  public void testBetweenQuery() throws Exception {
    Item item = item();
    item.with(COL1, 1);
    Table table = createTableWithItems(item);
    String query = selectStarWithPK("pk", "t", table) + " AND " +
                   "t." + COL1 + " BETWEEN 1 AND 2";
    verify(runAndReadResults(query), item);
    validatePlanWithQueries(query, of(pkEquals("pk"), create("between", COL1, 1, 2)));
  }

  @Test
  public void testBetweenUpdateRange() throws Exception {
    Item item = item();
    item.with(COL1, 5);
    Table table = createTableWithItems(item);
    String query = "SELECT *" + from(table) + "t WHERE " +
                   "t." + COL1 + " BETWEEN 1 AND 10 AND " +
                   "t." + COL1 + " >= 4 AND " +
                   "t." + COL1 + " <= 7";
    verify(runAndReadResults(query), item);
    validatePlanWithScan(query, create("between", COL1, 4, 7));
  }

  private String selectStarWithPK(String pk, String tableName, Table table) {
    return "SELECT *" + from(
      table) + tableName + " WHERE " + tableName + "." + PK + " = '" + pk + "'";
  }

  private void validatePlanWithScan(String query, DynamoFilterSpec scan) throws Exception {
    validatePlan(query, null, new DynamoReadFilterSpec(scan), null);
  }

  private void validatePlanWithGets(String query, DynamoFilterSpec... gets) throws Exception {
    validatePlan(query, null, null,
      Arrays.asList(gets).stream().map(DynamoGetFilterSpec::new).collect(
        Collectors.toList()));
  }

  private void validatePlanWithQueries(String query, Pair<DynamoFilterSpec, DynamoFilterSpec>...
    queries) throws
    Exception {
    validatePlan(query, null, null,
      Arrays.asList(queries).stream().map(p -> new DynamoQueryFilterSpec(p.getKey(), p.getValue()))
            .collect(Collectors.toList()));
  }

  private void validatePlan(String query, List<String> columns, DynamoReadFilterSpec scan,
    List<DynamoReadFilterSpec> getOrQuery) throws Exception {
    if (columns == null) {
      columns = newArrayList("`*`");
    }
    Map<String, Object> plan = justOneRow(runAndReadResults(explain(query)));
    Map<String, Object> json = MAPPER.readValue(plan.get("json").toString(), Map.class);
    List<Map<String, Object>> graph = (List<Map<String, Object>>) json.get("graph");
    Map<String, Object> dynamo = graph.get(0);
    assertEquals(DynamoGroupScan.NAME, dynamo.get("pop"));
    assertEquals(columns, dynamo.get("columns"));
    assertTrue((Boolean) dynamo.get("filterPushedDown"));
    Map<String, Object> spec = (Map<String, Object>) dynamo.get("spec");
    String specString = MAPPER.writeValueAsString(spec);
    DynamoGroupScanSpec gSpec = MAPPER.readValue(specString, DynamoGroupScanSpec.class);
    if (scan == null) {
      assertNull(gSpec.getScan());
      List<DynamoReadFilterSpec> actual = gSpec.getGetOrQuery();
      Collections.sort(actual, (spec1, spec2) -> spec1.toString().compareTo(spec2.toString()));
      assertEquals(getOrQuery, actual);
    } else {
      assertNull(gSpec.getGetOrQuery());
      assertEquals(scan, gSpec.getScan());
    }
  }

  private String explain(String sql) {
    return "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITH IMPLEMENTATION FOR " + sql;
  }

}
