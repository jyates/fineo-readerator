package io.fineo.read.drill.exec.store.schema;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

public class FineoSubSchemas {

  private Multimap<String, FsSourceTable> orgToTables = ArrayListMultimap.create();

  public FineoSubSchemas(Map<String, List<FsSourceTable>> sources) {
    for (Map.Entry<String, List<FsSourceTable>> entry : sources.entrySet()) {
      for (FsSourceTable table : entry.getValue()) {
        String org = entry.getKey();
        assert table.getOrg().equals(org) : "Source org and parent don't match!";
        if (!orgToTables.get(org).contains(table)) {
          this.orgToTables.put(entry.getKey(), table);
        }
      }
    }
  }

  public List<FsSourceTable> getSchemas(String org) {
    return newArrayList(orgToTables.get(org));
  }
}
