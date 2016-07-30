package io.fineo.read.drill.exec.store.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestFsSource {

  @Test
  public void testSerialize() throws Exception {
    FsSourceTable source = new FsSourceTable("json", "base");
    ObjectMapper mapper = new ObjectMapper();
    String val = mapper.writeValueAsString(source);
    assertEquals(source, mapper.readValue(val, FsSourceTable.class));
  }
}
