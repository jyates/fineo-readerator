package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import com.google.common.collect.Iterables;
import io.fineo.read.drill.exec.store.rel.recombinator.physical.Recombinator;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

/**
 * Called by reflection
 */
public class RecombinatorRecordBatchCreator implements BatchCreator<Recombinator> {
  @Override
  public RecombinatorRecordBatch getBatch(FragmentContext context, Recombinator config,
    List<RecordBatch> children)
    throws ExecutionSetupException {
    switch (config.getSourceType()) {
      case DFS:
      case DYNAMO:
      default:
        return new RecombinatorRecordBatch(config, context, Iterables.getOnlyElement(children));
    }
  }
}
