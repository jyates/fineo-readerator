package io.fineo.read.drill.exec.store.rel.expansion;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Called by reflection
 */
public class DynamoExpanderBatchCreator implements BatchCreator<DynamoExpander> {
  @Override
  public DynamoExpanderBatch getBatch(FragmentContext context, DynamoExpander config,
    List<RecordBatch> children)
    throws ExecutionSetupException {
    return new DynamoExpanderBatch(config, context, getOnlyElement(children));
  }
}
