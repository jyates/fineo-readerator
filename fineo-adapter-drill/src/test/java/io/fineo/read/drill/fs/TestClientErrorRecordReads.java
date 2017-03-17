package io.fineo.read.drill.fs;

import io.fineo.drill.ClusterTest;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

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

  private Map<String, Object> m1(){
    Map<String, Object> map = new HashMap<>();
    map.put("message", "Got an error");
    map.put(EVENT_KEY, "{\"the event\": \"event data\"}");
    return map;
  }

  private Map<String, Object> m2(){
    Map<String, Object> map2 = new HashMap<>();
    map2.put("message", "Got another error");
    map2.put(EVENT_KEY, "{\"the event\": \"more event data\"}");
    return map2;
  }
}
