package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class FineoSubSchemas {

  private final List<FsSourceTable> sources;

  public FineoSubSchemas(List<FsSourceTable> sources) {
    this.sources = sources;
  }

  public List<FsSourceTable> getSchemas() {
    return newArrayList(sources);
  }
}
