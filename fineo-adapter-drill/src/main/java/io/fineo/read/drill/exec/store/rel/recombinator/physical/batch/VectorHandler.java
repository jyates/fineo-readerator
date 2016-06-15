package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import org.apache.drill.exec.record.VectorWrapper;

/**
 *
 */
public interface VectorHandler {
  void copyField(VectorWrapper<?> wrapper, int inIndex, String sanitizedName);

  void reset();
}
