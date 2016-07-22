package io.fineo.read.drill.exec.store.schema;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import oadd.com.google.common.base.Joiner;

import java.util.List;
import java.util.Map;

public class SubTableScanBuilder {

  private static final Joiner PATH = Joiner.on("/");
  private final Multimap<String, FsSourceTable> sources = ArrayListMultimap.create();

  public SubTableScanBuilder(List<FsSourceTable> sourceFsTables) {
    for (FsSourceTable source : sourceFsTables) {
      sources.put(source.getSchema(), source);
    }
  }

  public void scan(LogicalScanBuilder builder, String metricId) {
    for (Map.Entry<String, FsSourceTable> schemas : sources.entries()) {
      String baseDir = getBaseDir(schemas.getValue(), metricId);
      builder.scan(schemas.getKey(), baseDir);
    }
  }

  private String getBaseDir(FsSourceTable table, String metricId) {
    //TODO replace this with a common "directory layout" across this and the BatchETL job.
    // Right now this is an implicit linkage and it would be nicer to formalize that (and add
    // safety by checking version)
    return PATH
      .join(table.getBasedir(), FineoStoragePlugin.VERSION, table.getFormat(), table.getOrg(),
        metricId);
  }
}
