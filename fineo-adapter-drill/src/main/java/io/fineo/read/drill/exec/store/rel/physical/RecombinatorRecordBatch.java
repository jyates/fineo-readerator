package io.fineo.read.drill.exec.store.rel.physical;

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.ProducerConsumer;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Do the actual work of transforming input records to the expected customer type
 */
public class RecombinatorRecordBatch extends AbstractSingleRecordBatch<Recombinator> {

  private final Map<String, List<String>> map;

  protected RecombinatorRecordBatch(final Recombinator popConfig, final FragmentContext context,
    final RecordBatch incoming) throws
    OutOfMemoryException {
    super(popConfig, context, incoming);
    this.map = popConfig.getMap();
  }

  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    return false;
  }

  @Override
  protected IterOutcome doWork() {
    return null;
  }

  @Override
  public int getRecordCount() {
    return 0;
  }
}
