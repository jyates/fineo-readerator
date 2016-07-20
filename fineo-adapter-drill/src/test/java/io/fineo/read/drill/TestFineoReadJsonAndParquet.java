package io.fineo.read.drill;


import com.google.common.io.Files;
import io.fineo.read.drill.exec.store.plugin.SourceFsTable;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.StoreClerk;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

public class TestFineoReadJsonAndParquet extends BaseFineoTest {

  @Test
  public void testReadParquet() throws Exception {
    TestState state = register();

    File tmp = folder.newFolder("drill");
    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, false);
    SourceFsTable parquet = writeParquet(state, tmp, org, metrictype, 1, values);

    // ensure that the fineo-test plugin is enabled
    bootstrap(parquet);

    verifySelectStar(result -> {
      assertNext(result, values);
    });
  }

  @Test
  public void testSingleJsonAndParquet() throws Exception {
    TestState state = register();

    File tmp = folder.newFolder("drill");
    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, false);
    SourceFsTable json = state.write(tmp, org, metrictype, 1, values);
    Map<String, Object> values2 = newHashMap(values);
    values2.put(fieldname, true);
    SourceFsTable parquet = writeParquet(state, tmp, org, metrictype, 2, values2);

    // ensure that the fineo-test plugin is enabled
    bootstrap(json, parquet);

    verifySelectStar(result -> {
      assertNext(result, values);
      assertNext(result, values2);
    });
  }

  private SourceFsTable writeParquet(TestState state, File dir, String orgid, String metricType,
    long ts, Map<String, Object>... rows) throws Exception {
    // set the values in the row
    StoreClerk clerk = new StoreClerk(state.store, org);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricType);
    for (Map<String, Object> row : rows) {
      setValues(row, orgid, metric, ts);
    }

    // write to a tmp json file
    File tmp = new File(dir, "tmp-json");
    if (!tmp.exists()) {
      assertTrue("Couldn't make the tmp directory: " + tmp, tmp.mkdirs());
    }
    File out = new File(tmp, format("%s-tmp-to-parquet.json", UUID.randomUUID()));
    writeJsonFile(out, rows);

    // create a parquet table
    String path = "dfs.`" + out + "`";
    String table = "tmp_parquet";
    String request = format("CREATE TABLE %s AS SELECT * from %s", table, path);
    String alter = "alter session set `store.format`='parquet'";
    String use = "use dfs.tmp";
    Connection conn = drill.getConnection();
    conn.createStatement().execute(alter);
    conn.createStatement().execute(use);
    conn.createStatement().execute(request);

    //copy the contents to the actual output file that we want to use for ingest
    SourceFsTable source = new SourceFsTable("parquet", dir.getPath(), org);
    File outputDir = createOutputDir(source, metric, ts);
    File from = new File("/tmp", table);
    Files.move(from, outputDir);
    return source;
  }
}
