package io.fineo.read.drill;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.avro.AvroFormatConfig;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.NamedFormatPluginConfig;
import org.apache.drill.exec.store.easy.json.JSONFormatPlugin;
import org.apache.drill.exec.store.easy.sequencefile.SequenceFileFormatConfig;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static junit.framework.TestCase.assertEquals;
import static org.apache.calcite.util.ImmutableNullableList.of;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Ensure that we push the timerange down into the scan when applicable
 */
public class TestFineoPushTimerange extends BaseFineoTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerSubtypes(FileSystemConfig.class);
    // all the different formats, which the EasyScan plugin serializes, for some reason
    MAPPER.registerSubtypes(JSONFormatPlugin.JSONFormatConfig.class,
      AvroFormatConfig.class,
      TextFormatPlugin.TextFormatConfig.class,
      NamedFormatPluginConfig.class,
      ParquetFormatConfig.class,
      SequenceFileFormatConfig.class
    );
    // SchemaPath doesn't have a good deserializer for some reason...
    MAPPER.registerSubtypes(FieldReference.class);
    SimpleModule module = new SimpleModule("schema-path-deserializer");
    module.addDeserializer(SchemaPath.class, new JsonDeserializer<SchemaPath>() {
      @Override
      public SchemaPath deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
        return SchemaPath.create(UserBitShared.NamePart.newBuilder().setName(p.getText()).build());
      }
    });
    MAPPER.registerModule(module);
  }

  private static final long ONE_DAY_MILLIS = 24 * 60 * 60 * 1000;

  @Test
  public void testPushTimerangeIntoFileQuery() throws Exception {
    TestState state = register();

    Map<String, Object> values = newHashMap();
    values.put(fieldname, false);
    File tmp = folder.newFolder("drill");
    Map<String, Object> values2 = newHashMap(values);
    long start = get1980();
    Pair<FsSourceTable, File> j1 = writeJsonAndGetOutputFile(state.store, tmp, org, metrictype,
      start, newArrayList(values));
    Pair<FsSourceTable, File> j2 = writeJsonAndGetOutputFile(state.store, tmp, org, metrictype,
      start + (ONE_DAY_MILLIS * 2), newArrayList(values));
    Pair<FsSourceTable, File> j3 = writeJsonAndGetOutputFile(state.store, tmp, org, metrictype,
      start + (ONE_DAY_MILLIS * 3), newArrayList(values2));

    // ensure that the fineo-test plugin is enabled
    bootstrap(j1.getKey(), j2.getKey(), j3.getKey());

    String query = verifySelectStar(ImmutableList.of("`timestamp` > " + start),
      result -> {
        assertNext(result, values);
        assertNext(result, values2);
      });

    // make sure that the base scan only uses 2 of the three possible files from the correct
    // partitions.
    Connection conn = drill.getConnection();
    String explain = explain(query);
    ResultSet plan = conn.createStatement().executeQuery(explain);
    assertTrue("After successful read, could not get the plan for query: " + explain, plan.next());
    String jsonPlan = plan.getString("json");
    Map<String, Object> jsonMap = MAPPER.readValue(jsonPlan, Map.class);
    List<Map<String, Object>> graph = (List<Map<String, Object>>) jsonMap.get("graph");
    Map<String, Object> scan = graph.get(0);

    File selectionRoot = getSelectionRoot(state.store, j1.getKey());
    validatePlan(scan, JSONFormatPlugin.JSONFormatConfig.class,
      prefixFilesWithFILE(j2.getValue(), j3.getValue()), selectionRoot, of("`*`"));
  }

  /**
   * Because of the way the PruneScanRule works we have to select at least one partition. Thus,
   * we can get the case where we don't actually select any rows (filtered out from incoming) BUT
   * we need to provide some sort of schema from the recombinator
   *
   * @throws Exception on failure
   */
  @Test
  public void testPushTimerangeIntoQuerySingleFile() throws Exception {
    TestState state = register();

    Map<String, Object> values = newHashMap();
    values.put(fieldname, false);
    File tmp = folder.newFolder("drill");
    long start = get1980();
    Pair<FsSourceTable, File> j1 = writeJsonAndGetOutputFile(state.store, tmp, org, metrictype,
      start, newArrayList(values));

    // ensure that the fineo-test plugin is enabled
    bootstrap(j1.getKey());

    String query = verifySelectStar(ImmutableList.of("`timestamp` > " + start),
      result -> assertFalse("Got a row when we shouldn't have!", result.next()));

    // check that we do, in fact, actually scan that one file... even though the partition
    // definitely excludes it.
    Connection conn = drill.getConnection();
    String explain = explain(query);
    ResultSet plan = conn.createStatement().executeQuery(explain);
    assertTrue("After successful read, could not get the plan for query: " + explain, plan.next());
    String jsonPlan = plan.getString("json");
    Map<String, Object> jsonMap = MAPPER.readValue(jsonPlan, Map.class);
    List<Map<String, Object>> graph = (List<Map<String, Object>>) jsonMap.get("graph");
    Map<String, Object> scan = graph.get(0);

    File selectionRoot = getSelectionRoot(state.store, j1.getKey());
    List<String> files = prefixFilesWithFILE(j1.getValue());
    validatePlan(scan, JSONFormatPlugin.JSONFormatConfig.class, files, selectionRoot, of("`*`"));
  }

  private List<String> prefixFilesWithFILE(File... files) {
    List<File> fl = newArrayList(files);
    return fl.stream().map(f -> "file:" + f).collect(Collectors.toList());
  }

  private File getSelectionRoot(SchemaStore store, FsSourceTable source)
    throws SchemaNotFoundException {
    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metrictype);
    File selectionRoot = new File(source.getBasedir(), "0");
    selectionRoot = new File(selectionRoot, source.getFormat());
    selectionRoot = new File(selectionRoot, org);
    return new File(selectionRoot, metric.getMetricId());
  }

  private void validatePlan(Map<String, Object> scan, Class<? extends FormatPluginConfig>
    pluginFormat, List<String> files, File selectionRoot, List<String> columns)
    throws IOException {
    assertEquals(files, scan.get("files"));

    String filePrefix = "file:";
    assertEquals(filePrefix + selectionRoot, scan.get("selectionRoot"));
    assertEquals(columns, scan.get("columns"));
    FormatPluginConfig format = MAPPER.readValue(MAPPER.writeValueAsString(scan.get("format")),
      FormatPluginConfig.class);
    assertEquals("Expected a json type format!", pluginFormat, format.getClass());
  }

  // yup, just gonna duplicate the above, but with parquet and json. we have more control over
  // exactly what gets validated and I don't have enough mental capacity to break this into
  // intelligible bite-size chunks right now. #startup
  @Test
  public void testPushTimerangeIntoMultipleFileQuery() throws Exception {
    TestState state = register();

    File tmp = folder.newFolder("drill");
    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, false);
    // filtering only appears to work if we have more than 1 partition, so create two json
    // partitions
    long start = get1980();
    Pair<FsSourceTable, File> json =
      writeJsonAndGetOutputFile(state.store, tmp, org, metrictype, start, of(values));
    Pair<FsSourceTable, File> json2 = writeJsonAndGetOutputFile(state.store, tmp, org, metrictype,
      start + (ONE_DAY_MILLIS * 2), of(values));

    // write parquet that is different
    Map<String, Object> values2 = newHashMap(values);
    values2.put(fieldname, true);
    Pair<FsSourceTable, File> parquet =
      writeParquet(state, tmp, org, metrictype, start, values2);
    Pair<FsSourceTable, File> parquet2 =
      writeParquet(state, tmp, org, metrictype, start + 11 + ONE_DAY_MILLIS * 2, values2);

    // enable the json and parquet formats. We only need the first one here b/c time is not
    // included in the information sent, but rather extracted from the filesystem layout
    bootstrap(json.getKey(), parquet.getKey());

    String query =
      verifySelectStar(ImmutableList.of("`timestamp` > " + start), withNext(values, values2));
    Connection conn = drill.getConnection();
    String explain = explain(query);
    ResultSet plan = conn.createStatement().executeQuery(explain);
    assertTrue("After successful read, could not get the plan for query: " + explain, plan.next());
    String jsonPlan = plan.getString("json");
    Map<String, Object> jsonMap = MAPPER.readValue(jsonPlan, Map.class);
    List<Map<String, Object>> graph = (List<Map<String, Object>>) jsonMap.get("graph");

    Map<String, Object> parquetScan = getGraphStep(graph, "parquet-scan");
    File selectionRoot = getSelectionRoot(state.store, parquet.getKey());
    List<String> files =
      of(parquet2.getValue()).stream().map(File::toString).collect(Collectors.toList());
    validatePlan(parquetScan, ParquetFormatConfig.class, files, selectionRoot, of("`*`"));

    Map<String, Object> jsonScan = getGraphStep(graph, "fs-scan");
    selectionRoot = getSelectionRoot(state.store, json.getKey());
    files = prefixFilesWithFILE(json2.getValue());
    validatePlan(jsonScan, JSONFormatPlugin.JSONFormatConfig.class, files, selectionRoot,
      of("`*`"));
  }

  private String explain(String sql) {
    return "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITH IMPLEMENTATION FOR " + sql;
  }

  private Map<String, Object> getGraphStep(List<Map<String, Object>> graph, String popName) {
    for (Map<String, Object> pop : graph) {
      if (pop.get("pop").equals(popName)) {
        return pop;
      }
    }
    return null;
  }
}
