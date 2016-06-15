package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import com.google.common.base.Preconditions;
import io.fineo.read.drill.exec.store.FineoCommon;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;

/**
 *
 */
public class DynamicVectorHandler implements VectorHandler {
  private final SingleMapWriter map;

  public DynamicVectorHandler(SingleMapWriter mapWriter) {
    this.map = mapWriter;
    // allocate a new space in the map for the input field
    map.allocate();
  }

  @Override
  public void copyField(VectorWrapper<?> wrapper, int inIndex, String sanitizedName) {
    // make sure that we are readying a real value
    ValueVector in = wrapper.getValueVector();

    // if there is a field named _fm for which we don't have an assignment, it might just be the
    // project layer below injecting a null. We just ignore that null and go to the next value
    if (wrapper.getField().getName().equals(FineoCommon.MAP_FIELD)) {
      if (in.getAccessor().isNull(inIndex)) {
        return;
      }
      return;
    }
    Preconditions
      .checkArgument(!in.getAccessor().isNull(inIndex), "Want to map a field that is set to null!");

    // switch for the type... probably better to do this as gen code...
    MaterializedField field = wrapper.getField();
    String name = sanitizedName;

    switch (field.getType().getMinorType()) {
      case VARCHAR:
        map.varChar(name).write(HolderUtil.holdVarChar((NullableVarCharVector) in, inIndex));
        break;
      case BIGINT:
        map.bigInt(name)
           .writeBigInt(((NullableBigIntVector) in).getAccessor().get(inIndex));
        break;
      case BIT:
        map.bit(name).writeBit(((NullableBitVector) in).getAccessor().get(inIndex));
        break;
      default:
        throw new UnsupportedOperationException("Cannot convert field: " + field);
    }
  }

  @Override
  public void reset() {
    // noop
  }
}
