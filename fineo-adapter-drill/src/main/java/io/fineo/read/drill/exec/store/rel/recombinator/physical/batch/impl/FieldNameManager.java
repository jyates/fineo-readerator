package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch.impl;

/**
 *
 */
public interface FieldNameManager {
  String getOutputName(String upstreamFieldName);
}
