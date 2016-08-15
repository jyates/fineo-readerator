package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import io.fineo.read.drill.exec.store.rel.recombinator.physical.Recombinator;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Called by reflection
 */
public class RecombinatorRecordBatchCreator implements BatchCreator<Recombinator> {
  @Override
  public RecombinatorRecordBatch getBatch(FragmentContext context, Recombinator config,
    List<RecordBatch> children)
    throws ExecutionSetupException {
    switch (config.getSourceType()) {
      case DYNAMO:
      case DFS:
      default:
        return new RecombinatorRecordBatch(config, context, getOnlyElement(children));
    }
  }
}
