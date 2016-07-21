package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import com.google.common.base.Preconditions;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
class Mutator implements OutputMutator {
  private Map<String, ValueVector> fieldVectorMap = new HashMap<>();
  private final AliasFieldNameManager aliasMap;
  private final OperatorContext oContext;
  private final SchemaChangeCallBack callBack;
  private final VectorContainer container;
  /**
   * Whether schema has changed since last inquiry (via #isNewSchema}).  Is
   * true before first inquiry.
   */
  private boolean schemaChanged = true;

  Mutator(AliasFieldNameManager aliasMap, OperatorContext oContext, SchemaChangeCallBack callBack,
    VectorContainer container) {
    this.aliasMap = aliasMap;
    this.oContext = oContext;
    this.callBack = callBack;
    this.container = container;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends ValueVector> T addField(MaterializedField field,
    Class<T> clazz) throws SchemaChangeException {
    String name = field.getName();
    String outputName = aliasMap.getOutputName(name);
    Preconditions.checkNotNull(outputName,
      "Didn't find an output name for: %s, it should be handled as a dynamic _fm field!", name);
    // Check if the field exists.
    ValueVector v = fieldVectorMap.get(outputName);
    if (v == null || v.getClass() != clazz) {
      // Field does not exist--add it to the map and the output container.
      v = TypeHelper.getNewVector(field, oContext.getAllocator(), callBack);
      if (!clazz.isAssignableFrom(v.getClass())) {
        throw new SchemaChangeException(
          String.format(
            "The class that was provided, %s, does not correspond to the "
            + "expected vector type of %s.",
            clazz.getSimpleName(), v.getClass().getSimpleName()));
      }

      final ValueVector old = fieldVectorMap.put(field.getPath(), v);
      if (old != null) {
        old.clear();
        container.remove(old);
      }

      container.add(v);
      // Added new vectors to the container--mark that the schema has changed.
      schemaChanged = true;
    }

    return clazz.cast(v);
  }

  @Override
  public void allocate(int recordCount) {
    for (final ValueVector v : fieldVectorMap.values()) {
      AllocationHelper.allocate(v, recordCount, 1, 0);
    }
  }

  /**
   * Reports whether schema has changed (field was added or re-added) since
   * last call to {@link #isNewSchema}.  Returns true at first call.
   */
  @Override
  public boolean isNewSchema() {
    // Check if top-level schema or any of the deeper map schemas has changed.

    // Note:  Callback's getSchemaChangedAndReset() must get called in order
    // to reset it and avoid false reports of schema changes in future.  (Be
    // careful with short-circuit OR (||) operator.)

    final boolean deeperSchemaChanged = callBack.getSchemaChangedAndReset();
    if (schemaChanged || deeperSchemaChanged) {
      schemaChanged = false;
      return true;
    }
    return false;
  }

  @Override
  public DrillBuf getManagedBuffer() {
    return oContext.getManagedBuffer();
  }

  @Override
  public CallBack getCallBack() {
    return callBack;
  }

  public void setRecordCount(int incomingRecordCount) {
    for (final ValueVector v : fieldVectorMap.values()) {
      v.getMutator().setValueCount(incomingRecordCount);
    }
  }
}
