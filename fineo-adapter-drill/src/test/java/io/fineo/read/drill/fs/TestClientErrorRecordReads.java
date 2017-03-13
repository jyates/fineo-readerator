package io.fineo.read.drill.fs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.FineoDrillStartupSetup;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static jersey.repackaged.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertTrue;

/**
 * Reads records from the errors table as a client would read them
 */
public class TestClientErrorRecordReads extends BaseFineoTest {

  private static final String APIKEY_KEY = "apikey";
  private static final String EVENT_KEY = "event";

  @ClassRule
  public static TestOutput FOLDERS = new TestOutput(false);
  private static File ERRORS_DIR;
  private static File STREAM_DIR;
  private static File RAW_STAGE_DIR;
  private static File STAGED_STAGE_DIR;

  @BeforeClass
  public static void prepareCluster() throws Exception {
    FineoDrillStartupSetup setup = new FineoDrillStartupSetup(drill.getConnection());
    setup.run();

    RAW_STAGE_DIR = FOLDERS.newFolder("drill", "errors", "stream", "raw");
    STREAM_DIR = RAW_STAGE_DIR.getParentFile();
    STAGED_STAGE_DIR= new File(STREAM_DIR, "staged");
    ERRORS_DIR = STREAM_DIR.getParentFile();
  }

  private enum ErrorType {
    ERROR("error"), MALFORMED("malformed");

    private final String dir;

    ErrorType(String dir) {
      this.dir = dir;
    }

    public File getDir(File parent) {
      File dir = new File(parent, this.dir);
      if (!dir.exists()) {
        assertTrue("Failed to create " + dir, dir.mkdir());
      }
      return dir;
    }
  }

  @Before
  public void bootstrap() throws Exception {
    BootstrapFineo bootstrap = newBootstrap();
    BootstrapFineo.DrillConfigBuilder builder = simpleBootstrap(bootstrap.builder());
    builder.withError().withTable(new FsSourceTable("json", ERRORS_DIR.getAbsolutePath())).done();
    assertTrue("Failed to bootstrap drill!", bootstrap.strap(builder));

    register(); // ensures that we have a tenant schema

    // clean out any files from the last test in the errors directory
    FileUtils.deleteDirectory(RAW_STAGE_DIR);
    assertTrue(RAW_STAGE_DIR.mkdir());

    FileUtils.deleteDirectory(STAGED_STAGE_DIR);
    assertTrue(STAGED_STAGE_DIR.mkdir());
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

  private void write(int year, int month, int day, boolean zip, Map<String, Object>... events)
    throws FileNotFoundException, JsonProcessingException {
    write(year, month, day, zip, ErrorType.ERROR, events);
  }

  private void write(int year, int month, int day, boolean zip, ErrorType err,
    Map<String, Object>... events) throws FileNotFoundException, JsonProcessingException {
    write(year, month, day, zip, err, RAW_STAGE_DIR, events);
  }

  private void write(int year, int month, int day, boolean zip, ErrorType err, File stage,
    Map<String, Object>... events) throws FileNotFoundException, JsonProcessingException {
    for (Map<String, Object> event : events) {
      event.put(APIKEY_KEY, org);
    }
    // get the directory in which to write
    File errDir = err.getDir(stage);
    File yrDir = new File(errDir, Integer.toString(year));
    File monthDir = new File(yrDir, Integer.toString(month));
    File dayDir = new File(monthDir, Integer.toString(day));
    if (!dayDir.exists()) {
      assertTrue("Could not create day dir! Dir: " + dayDir, dayDir.mkdirs());
    }

    // write the events in the file
    String ending = zip ? "gzip" : "json";
    File out = new File(dayDir, format("archive-%s-%s-%s.%s", year, month, day, ending));
    // write each of the files, potentially through a gzip output stream
    PrintStream print = new PrintStream(out);
    ObjectMapper mapper = new ObjectMapper();
    for (Map<String, Object> event : events) {
      print.println(mapper.writeValueAsString(event));

      // update the event with the expected results
      event.put("stage", stage.getName());
      event.put("type", errDir.getName());
      event.put("year", Integer.toString(year));
      event.put("month", Integer.toString(month));
      event.put("day", Integer.toString(day));
    }
    print.close();
  }


  private void expectEvents(Map<String, Object>... events) throws Exception {
    expectEvents(newArrayList("raw"), newArrayList(ErrorType.ERROR), events);
  }

  private void expectEvents(List<ErrorType> errs,
    Map<String, Object>... events) throws Exception {
    expectEvents(newArrayList("raw"), errs, events);
  }

  private void expectEvents(List<String> stageNames, List<ErrorType> errs,
    Map<String, Object>... events) throws Exception {
    String stages = whereEqualsStringOnOr("stage", stageNames.stream());
    String types = whereEqualsStringOnOr("type", errs.stream().map(err -> err.dir));

    List<String> where = newArrayList(stages, types);
    QueryRunnable runnable = new QueryRunnable(where, FineoTestUtil.withNext(events));
    // read the json file in that directory
    runnable = runnable.from("errors.stream").sortBy("day");
    runAndVerify(runnable);
  }

  private String whereEqualsStringOnOr(String column, Stream<String> iter){
    return iter.map(s ->  format("%s='%s'", column, s)).collect(Collectors.joining(" OR "));
  }
}
