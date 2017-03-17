package io.fineo.read.drill.fs;

import io.fineo.drill.ClusterTest;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.FineoDrillStartupSetup;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static jersey.repackaged.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertTrue;

/**
 * Does raw queries against some error JSON records without making a query rewrite. These are the
 * actual queries that should be run against Drill, rather than the ones that get rewritten as in
 * {@link TestClientErrorRecordReads}
 */
@Category(ClusterTest.class)
public class TestReadRawErrorRecords extends BaseFineoTest{

  private static final String APIKEY_KEY = "apikey";
  private static final String EVENT_KEY = "event";
  private final String apikey = "somekey";

  @BeforeClass
  public static void prepareCluster() throws Exception {
    FineoDrillStartupSetup setup = new FineoDrillStartupSetup(drill.getConnection());
    setup.run();
  }

  @Test
  public void testSimpleReadFile() throws Exception {
    register(); // ensures that we have a tenant schema
    BootstrapFineo bootstrap = newBootstrap();
    BootstrapFineo.DrillConfigBuilder builder = simpleBootstrap(bootstrap.builder());
    String path = getPath("errors/error-root.marker");
    builder.withError().withTable(new FsSourceTable("json", path)).done();
    assertTrue("Failed to bootstrap drill!", bootstrap.strap(builder));

    // now attempt to read the file
    expectEvents("{\"the event\": \"event data\"}","{\"the event\": \"more data\"}");
  }

  @Test
  public void testReadGzipData() throws Exception {
    register(); // ensures that we have a tenant schema
    BootstrapFineo bootstrap = newBootstrap();
    BootstrapFineo.DrillConfigBuilder builder = simpleBootstrap(bootstrap.builder());
    String path = getPath("errors-gzip/error-gzip-root.marker");
    builder.withError().withTable(new FsSourceTable("json", path)).done();
    assertTrue("Failed to bootstrap drill!", bootstrap.strap(builder));

    // now attempt to read the file
    expectEvents("{\"the event\": \"event data\"}","{\"the event\": \"more data\"}");
  }

  private String getPath(String name){
    ClassLoader classLoader = getClass().getClassLoader();
    // parent file gives us the directory
    File file = new File(classLoader.getResource(name).getFile()).getParentFile();
    return file.getAbsolutePath();
  }

  private void expectEvents(String ... events) throws Exception {
    List<Map<String, Object>> expected = new ArrayList<>();
    for(String event: events){
      Map<String, Object> ex = new HashMap<>();
      ex.put(APIKEY_KEY, apikey);
      ex.put(EVENT_KEY, event);
      ex.put("stage", "raw");
      ex.put("type", "error");
      ex.put("year", "2016");
      ex.put("month", "10");
      ex.put("day", "12");
      expected.add(ex);
    }
    List<String> where = newArrayList("stage = 'raw' AND type = 'error'");
    QueryRunnable runnable = new QueryRunnable(where, FineoTestUtil.withNext(expected));
    // don't sort the results, its not a standard fineo query...yet
    runnable.sorts.clear();
    runnable.rewrite = false;
    runnable.withUnion = false;
    // read the json file in that directory
    runnable = runnable.from("`fineo`.`errors`.`stream`");
    runAndVerify(runnable);
  }
}
