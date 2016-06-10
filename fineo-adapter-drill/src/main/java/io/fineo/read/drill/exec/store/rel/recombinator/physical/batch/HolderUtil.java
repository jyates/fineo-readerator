package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;

class HolderUtil {

  private HolderUtil() {
  }

  static void copyVarchar(VectorWrapper<?> wrapper, ValueVector out, int index, int outdex) {
    NullableVarCharVector in = (NullableVarCharVector) wrapper.getValueVector();
    NullableVarCharHolder holder = holdNullVarChar(in, index);
    if (!(holder.isSet == 0)) {
      ((NullableVarCharVector) out).getMutator()
                                   .setSafe((outdex), holder.isSet, holder.start, holder.end,
                                     holder.buffer);
    }
  }

  private static NullableVarCharHolder holdNullVarChar(NullableVarCharVector in, int index) {
    NullableVarCharHolder out = new NullableVarCharHolder();
    out.isSet = in.getAccessor().isSet((index));
    if (out.isSet == 1) {
      out.buffer = in.getBuffer();
      long startEnd = in.getAccessor().getStartEnd((index));
      out.start = ((int) startEnd);
      out.end = ((int) (startEnd >> 32));
    }
    return out;
  }

  static VarCharHolder holdVarChar(NullableVarCharVector in, int index) {
    VarCharHolder out = new VarCharHolder();
    out.buffer = in.getBuffer();
    long startEnd = in.getAccessor().getStartEnd((index));
    out.start = ((int) startEnd);
    out.end = ((int) (startEnd >> 32));
    return out;
  }

  static void copyBigInt(VectorWrapper<?> wrapper, ValueVector out, int inIndex, int outdex) {
    NullableBigIntVector in = (NullableBigIntVector) wrapper.getValueVector();
    NullableBigIntHolder holder = new NullableBigIntHolder();
    {
      holder.isSet = in.getAccessor().isSet((inIndex));
      if (holder.isSet == 1) {
        holder.value = in.getAccessor().get((inIndex));
      }
    }
    if (!(holder.isSet == 0)) {
      ((NullableBigIntVector) out).getMutator().set((outdex), holder.isSet, holder.value);
    }
  }

  static void copyBit(VectorWrapper<?> wrapper, ValueVector out, int inIndex, int outdex) {
    NullableBitVector in = (NullableBitVector) wrapper.getValueVector();
    NullableBitHolder holder = new NullableBitHolder();
    {
      holder.isSet = in.getAccessor().isSet((inIndex));
      if (holder.isSet == 1) {
        holder.value = in.getAccessor().get((inIndex));
      }
    }
    if (!(holder.isSet == 0)) {
      ((NullableBitVector) out).getMutator().set((outdex), holder.isSet, holder.value);
    }
  }
}
