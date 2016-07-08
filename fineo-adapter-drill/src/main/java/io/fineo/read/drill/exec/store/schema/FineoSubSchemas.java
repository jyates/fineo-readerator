package io.fineo.read.drill.exec.store.schema;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.read.drill.exec.store.plugin.SourceFsTable;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

public class FineoSubSchemas {

  private Multimap<String, SourceFsTable> orgToTables = ArrayListMultimap.create();

  public FineoSubSchemas(Map<String, List<SourceFsTable>> sources) {
    for (Map.Entry<String, List<SourceFsTable>> entry : sources.entrySet()) {
      for (SourceFsTable table : entry.getValue()) {
        String org = entry.getKey();
        assert table.getOrg().equals(org) : "Source org and parent don't match!";
        if (!orgToTables.get(org).contains(table)) {
          this.orgToTables.put(entry.getKey(), table);
        }
      }
    }
  }

  public List<SourceFsTable> getSchemas(String org) {
    return newArrayList(orgToTables.get(org));
  }
}
