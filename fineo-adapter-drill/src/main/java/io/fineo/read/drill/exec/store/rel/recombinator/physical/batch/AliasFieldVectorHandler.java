package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copy a single transfer field
 */
public class AliasFieldVectorHandler implements VectorHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AliasFieldVectorHandler.class);
  private boolean transferred;
  private final ValueVector out;

  public AliasFieldVectorHandler(ValueVector out) {
    this.out = out;
  }

  @Override
  public void copyField(VectorWrapper<?> wrapper, int inIndex, String sanitizedName) {
    int outIndex = 0;
    if (wrapper.getValueVector().getAccessor().isNull(inIndex)) {
      LOG.debug("Skipping transfering vector for {} because it is empty", wrapper);
      return;
    }
    assert transferred == false : "Already transferred values for " + wrapper;
    transferred = true;

    MaterializedField field = wrapper.getField();
    switch (field.getType().getMinorType()) {
      case VARCHAR:
        HolderUtil.copyVarchar(wrapper, out, inIndex, outIndex);
        break;
      case BIGINT:
        HolderUtil.copyBigInt(wrapper, out, inIndex, outIndex);
        break;
      case BIT:
        HolderUtil.copyBit(wrapper, out, inIndex, outIndex);
        break;
      case VARBINARY:
        HolderUtil.copyBinary(wrapper, out, inIndex, outIndex);
        break;
      case FLOAT4:
        HolderUtil.copyFloat4(wrapper, out, inIndex, outIndex);
        break;
      default:
        throw new UnsupportedOperationException("Cannot convert field: " + field);
    }
  }

  @Override
  public void reset(){
    this.transferred = false;
  }
}
