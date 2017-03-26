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
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static jersey.repackaged.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertTrue;

public class BaseFineoTestWithErrorReads extends BaseFineoTest {
  protected static final String APIKEY_KEY = "apikey";
  protected static final String EVENT_KEY = "event";

  @ClassRule
  public static TestOutput FOLDERS = new TestOutput(false);
  protected static File ERRORS_DIR;
  protected static File STREAM_DIR;
  protected static File RAW_STAGE_DIR;
  protected static File STAGED_STAGE_DIR;
  protected static File ERROR_MARKER;

  protected enum ErrorType {
    ERROR("error"), MALFORMED("malformed");

    public final String name;

    ErrorType(String dir) {
      this.name = dir;
    }

    public File getDir(File parent) {
      File dir = new File(parent, this.name);
      if (!dir.exists()) {
        assertTrue("Failed to create " + dir, dir.mkdir());
      }
      return dir;
    }
  }

  @BeforeClass
  public static void prepareCluster() throws Exception {
    FineoDrillStartupSetup setup = new FineoDrillStartupSetup(drill.getConnection());
    setup.run();

    RAW_STAGE_DIR = FOLDERS.newFolder("drill", "errors", "stream", "raw");
    STREAM_DIR = RAW_STAGE_DIR.getParentFile();
    ERROR_MARKER = new File(STREAM_DIR, "error-marker.json");
    // the marker file is required b/c json doesn't support directory based data reads.
    // Therefore, it needs to have a data file before Drill recognizes it as a table
    assertTrue("Could not create error marker file", ERROR_MARKER.createNewFile());
    STAGED_STAGE_DIR = new File(STREAM_DIR, "staged");
    ERRORS_DIR = STREAM_DIR.getParentFile();
  }

  protected static void cleanupErrorDirs() throws IOException {
    FileUtils.deleteDirectory(RAW_STAGE_DIR);
    assertTrue(RAW_STAGE_DIR.mkdir());

    FileUtils.deleteDirectory(STAGED_STAGE_DIR);
    assertTrue(STAGED_STAGE_DIR.mkdir());
  }

  protected BootstrapFineo.DrillConfigBuilder bootstrapErrors(
    BootstrapFineo.DrillConfigBuilder builder) {
    return builder.withError().withTable(new FsSourceTable("json", ERRORS_DIR.getAbsolutePath()))
                  .done();
  }

  @Override
  protected BootstrapFineo.DrillConfigBuilder bootstrapper() {
    return bootstrapErrors(super.bootstrapper());
  }

  @Override
  protected void bootstrap(FsSourceTable... files) throws IOException {
    BootstrapFineo bootstrap = newBootstrap();
    BootstrapFineo.DrillConfigBuilder builder = simpleBootstrap(bootstrap.builder());

    for (FsSourceTable file : files) {
      builder.withLocalSource(file);
    }
    assertTrue("Failed to bootstrap drill!", bootstrap.strap(bootstrapErrors(builder)));
  }

  protected void write(int year, int month, int day, boolean zip, Map<String, Object>... events)
    throws FileNotFoundException, JsonProcessingException {
    write(year, month, day, zip, ErrorType.ERROR, events);
  }

  protected void write(int year, int month, int day, boolean zip, ErrorType err,
    Map<String, Object>... events) throws FileNotFoundException, JsonProcessingException {
    write(year, month, day, zip, err, RAW_STAGE_DIR, events);
  }

  protected void write(int year, int month, int day, boolean zip, ErrorType err, File stage,
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


  protected void expectEvents(Map<String, Object>... events) throws Exception {
    expectEvents(newArrayList("raw"), newArrayList(ErrorType.ERROR), events);
  }

  protected void expectEvents(List<ErrorType> errs,
    Map<String, Object>... events) throws Exception {
    expectEvents(newArrayList("raw"), errs, events);
  }

  protected void expectEvents(List<String> stageNames, List<ErrorType> errs,
    Map<String, Object>... events) throws Exception {
    String stages = whereEqualsStringOnOr("stage", stageNames.stream());
    String types = whereEqualsStringOnOr("type", errs.stream().map(err -> err.name));

    List<String> where = newArrayList(stages, types);
    QueryRunnable runnable = new QueryRunnable(where, FineoTestUtil.withNext(events));
    // read the json file in that directory
    runnable = runnable.from("errors.stream").sortBy("day");
    runAndVerify(runnable);
  }

  protected String whereEqualsStringOnOr(String column, Stream<String> iter) {
    return iter.map(s -> format("%s='%s'", column, s)).collect(Collectors.joining(" OR "));
  }
}
