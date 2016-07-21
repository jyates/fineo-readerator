package io.fineo.read.drill;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import io.fineo.read.drill.exec.store.plugin.SourceFsTable;
import io.fineo.schema.store.StoreClerk;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.avro.AvroFormatConfig;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.NamedFormatPluginConfig;
import org.apache.drill.exec.store.dfs.easy.EasyGroupScan;
import org.apache.drill.exec.store.easy.json.JSONFormatPlugin;
import org.apache.drill.exec.store.easy.sequencefile.SequenceFileFormatConfig;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static junit.framework.TestCase.assertEquals;
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
        throws IOException, JsonProcessingException {
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
    long start = LocalDate.of(1980, 1, 1).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
    Pair<SourceFsTable, File> j1 = writeJsonAndGetOutputFile(state.store, tmp, org, metrictype,
      start, newArrayList(values));
    Pair<SourceFsTable, File> j2 = writeJsonAndGetOutputFile(state.store, tmp, org, metrictype,
      start + (ONE_DAY_MILLIS * 2), newArrayList(values));
    Pair<SourceFsTable, File> j3 = writeJsonAndGetOutputFile(state.store, tmp, org, metrictype,
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
    List<String> files = (List<String>) scan.get("files");
    String filePrefix = "file:";
    assertEquals(newArrayList(j2.getValue().toString(), j3.getValue().toString()).stream().map(
      f -> filePrefix + f).collect(Collectors.toList()), files);


    StoreClerk clerk = new StoreClerk(state.store, org);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metrictype);
    String selectionRoot = (String) scan.get("selectionRoot");
    File file = new File(j1.getKey().getBasedir(), "0");
    file = new File(file, j1.getKey().getFormat());
    file = new File(file, j1.getKey().getOrg());
    file = new File(file, metric.getMetricId());
    assertEquals(filePrefix + file, selectionRoot);
    List<String> columns = (List<String>) scan.get("columns");
    assertEquals(newArrayList("`*`"), columns);
    FormatPluginConfig format = MAPPER.readValue(MAPPER.writeValueAsString(scan.get("format")),
      FormatPluginConfig.class);
    assertTrue("Expected a json type format!", format instanceof JSONFormatPlugin.JSONFormatConfig);
  }

  @Test
  public void testPushTimerangeIntoMultipleFileQuery() throws Exception {
    TestState state = register();

    File tmp = folder.newFolder("drill");
    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, false);
    // filtering only appears to work if we have more than 1 partition, so create two json
    // partitions
    long start = Instant.from(LocalDate.of(1980, 1, 1)).toEpochMilli();
    SourceFsTable json = state.write(tmp, org, metrictype, start, values);
    SourceFsTable json2 = state.write(tmp, org, metrictype, start + (ONE_DAY_MILLIS * 2), values);
    SourceFsTable json3 = state.write(tmp, org, metrictype, start + (ONE_DAY_MILLIS * 3), values);

    // write parquet that is different
    Map<String, Object> values2 = newHashMap(values);
    values2.put(fieldname, true);
    SourceFsTable parquet = writeParquet(state, tmp, org, metrictype, ONE_DAY_MILLIS * 2, values2);

    // ensure that the fineo-test plugin is enabled
    bootstrap(json, json2, parquet);

    String query = verifySelectStar(ImmutableList.of("`timestamp` > " + ONE_DAY_MILLIS), result
      -> {
      assertNext(result, values);
      assertNext(result, values2);
    });
    Connection conn = drill.getConnection();
    String explain = explain(query);
    ResultSet plan = conn.createStatement().executeQuery(explain);
    System.out.println(plan);
  }

  private String explain(String sql) {
    return "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITH IMPLEMENTATION FOR " + sql;
  }
}
