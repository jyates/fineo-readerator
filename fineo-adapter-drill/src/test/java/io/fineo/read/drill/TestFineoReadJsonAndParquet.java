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
}
