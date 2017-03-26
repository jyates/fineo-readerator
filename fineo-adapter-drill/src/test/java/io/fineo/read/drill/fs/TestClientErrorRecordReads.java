package io.fineo.read.drill.fs;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.drill.ClusterTest;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static jersey.repackaged.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertTrue;

/**
 * Reads records from the errors table as a client would read them
 */
@Category(ClusterTest.class)
public class TestClientErrorRecordReads extends BaseFineoTestWithErrorReads {

  @Before
  public void bootstrap() throws Exception {
    BootstrapFineo bootstrap = newBootstrap();
    BootstrapFineo.DrillConfigBuilder builder = simpleBootstrap(bootstrap.builder());
    bootstrapErrors(builder);
    assertTrue("Failed to bootstrap drill!", bootstrap.strap(builder));

    register(); // ensures that we have a tenant schema

    // clean out any files from the last test in the errors directory
    cleanupErrorDirs();
  }

  @Test
  public void testSimpleReadFile() throws Exception {
    Map<String, Object> map = m1();
    write(2016, 10, 3, false, map);
    expectEvents(map);
  }

  @Test
  public void testReadGzipFile() throws Exception {
    Map<String, Object> map = m1();
    write(2016, 10, 3, true, map);
    expectEvents(map);
  }

  @Test
  public void testMultipleRowsInSameFile() throws Exception {
    Map<String, Object> map = m1();
    Map<String, Object> map2 = m2();
    write(2016, 10, 3, true, map, map2);
    expectEvents(map, map2);
  }

  @Test
  public void testRowsInDifferentDirectories() throws Exception {
    Map<String, Object> map = m1();
    Map<String, Object> map2 = m2();
    write(2016, 10, 3, true, map);
    write(2016, 10, 4, true, map2); // one day later
    expectEvents(map, map2);
  }

  @Test
  public void testRowsInDifferentDirectoriesAndMixedZip() throws Exception {
    Map<String, Object> map = m1();
    Map<String, Object> map2 = m2();
    write(2016, 10, 3, true, map);
    write(2016, 10, 4, false, map2); // one day later
    expectEvents(map, map2);
  }

  @Test
  public void testDifferentErrorTypes() throws Exception {
    Map<String, Object> map = m1();
    Map<String, Object> map2 = m2();
    write(2016, 10, 3, true, ErrorType.ERROR, map);
    write(2016, 10, 4, true, ErrorType.MALFORMED, map2); // one day later, to help sorting
    expectEvents(newArrayList(ErrorType.ERROR, ErrorType.MALFORMED), map, map2);
  }

  @Test
  public void testReadDifferentStages() throws Exception {
    Map<String, Object> map = m1();
    Map<String, Object> map2 = m2();
    write(2016, 10, 3, true, ErrorType.ERROR, RAW_STAGE_DIR, map);
    write(2016, 10, 4, true, ErrorType.MALFORMED, STAGED_STAGE_DIR, map2);
    expectEvents(newArrayList("raw", "staged"),
      newArrayList(ErrorType.ERROR, ErrorType.MALFORMED),
      map, map2);
  }

  /**
   * This isn't quite what happens in prod - we are forcing the error marker to sit on the query
   * path. The path specifies stage and type, so it skips past the 'stream' directory (where
   * the marker file usually sits). Here, we place that error marker in the stream/raw/error
   * directory and ensures it gets read.
   * @throws Exception
   */
  @Test
  public void testReadRowsWithErrorMarker() throws Exception {
    Map<String, Object> marker = new HashMap<>();
    marker.put("apikey", "===empty===");
    File out = new File(STREAM_DIR, "error-marker.json");
    try (PrintStream print = new PrintStream(out)) {
      ObjectMapper mapper = new ObjectMapper();
      print.println(mapper.writeValueAsString(marker));
    }

    Map<String, Object> map = m1();
    write(2016, 10, 3, true, ErrorType.ERROR, RAW_STAGE_DIR, map);

    List<String> where = newArrayList(format("apikey ='%s'", org));
    QueryRunnable runnable = new QueryRunnable(where, FineoTestUtil.withNext(map));
    // read the json file in that directory
    runnable = runnable.from("errors.stream");
    runnable.sorts.clear();
    runnable.withUnion = false;
    runAndVerify(runnable);
  }

  private Map<String, Object> m1() {
    Map<String, Object> map = new HashMap<>();
    map.put("message", "Got an error");
    map.put(EVENT_KEY, "{\"the event\": \"event data\"}");
    return map;
  }

  private Map<String, Object> m2() {
    Map<String, Object> map2 = new HashMap<>();
    map2.put("message", "Got another error");
    map2.put(EVENT_KEY, "{\"the event\": \"more event data\"}");
    return map2;
  }
}
