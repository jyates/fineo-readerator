package io.fineo.read.drill.fs;


import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

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
    FsSourceTable parquet = writeParquet(state, tmp, org, metrictype, 1, values).getKey();

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
    FsSourceTable json = state.write(tmp, org, metrictype, 1, values);
    Map<String, Object> values2 = newHashMap(values);
    values2.put(fieldname, true);
    FsSourceTable parquet = writeParquet(state, tmp, org, metrictype, 2, values2).getKey();

    // ensure that the fineo-test plugin is enabled
    bootstrap(json, parquet);

    verifySelectStar(result -> {
      assertNext(result, values);
      assertNext(result, values2);
    });
  }
}
