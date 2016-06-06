package io.fineo.read.drill.exec.store.rel.physical.batch;

import io.fineo.schema.Pair;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Combines multiple potential sub-fields into a single know field
 */
public class FieldTransferMapper {

  private Map<String, ValueVector> fieldMapping = new HashMap<>();
  private List<Pair<VectorWrapper, ValueVector>> transfers = new ArrayList<>();

  /**
   * Do the actual work of mapping the incoming records to the outgoing vectors
   *
   * @param incomingIndex
   */
  public void combine(int incomingIndex) {
    for (Pair<VectorWrapper, ValueVector> p : transfers) {
      copy(p.getKey(), p.getValue(), incomingIndex, 0);
    }
  }

  /**
   * Finds all the matching output vectors and makes a new transfer pair for the incoming vectors
   * based on the name of the field (assumed to be unique from the underlying projection wrapper
   * around the scan
   *
   * @param in batch to read
   * @return list of ownership transfers for the vectors in the group
   */
  public List<TransferPair> prepareTransfers(RecordBatch in) {
    this.transfers.clear();
    List<TransferPair> transfers = new ArrayList<>();
    for (VectorWrapper<?> wrapper : in) {
      MaterializedField field = wrapper.getField();
      String name = field.getName();
      ValueVector out = getOutput(name);
      transfers.add(wrapper.getValueVector().makeTransferPair(out));
      this.transfers.add(new Pair<>(wrapper, out));
    }
    return transfers;
  }

  public void addField(String inputName, ValueVector outputName) {
    fieldMapping.put(inputName, outputName);
  }

  public void addField(List<String> inputName, ValueVector vvOut) {
    // manage fields that we don't know about
    if (inputName == null) {
      addField((String) null, vvOut);
      return;
    }
    for (String in : inputName) {
      addField(in, vvOut);
    }
  }

  private ValueVector getOutput(String incoming) {
    ValueVector value = this.fieldMapping.get(incoming);
    // unknown field type
    if (value == null) {
      value = this.fieldMapping.get(null);
    }
    return value;
  }

  // TODO replace all of this with generated code
  private void copy(VectorWrapper<?> wrapper, ValueVector out, int inIndex, int outIndex) {
    MaterializedField field = wrapper.getField();
    switch (field.getType().getMinorType()) {
      case VARCHAR:
        copyVarchar(wrapper, out, inIndex, outIndex);
        break;
      case BIGINT:
        copyBigInt(wrapper, out, inIndex, outIndex);
        break;
      case BIT:
        copyBit(wrapper, out, inIndex, outIndex);
        break;
      default:
        throw new UnsupportedOperationException("Cannot convert field: " + field);
    }
  }

  private void copyVarchar(VectorWrapper<?> wrapper, ValueVector out, int index, int outdex) {
    NullableVarCharVector in = (NullableVarCharVector) wrapper.getValueVector();
    NullableVarCharHolder out3 = new NullableVarCharHolder();
    out3.isSet = in.getAccessor().isSet((index));
    if (out3.isSet == 1) {
      out3.buffer = in.getBuffer();
      long startEnd = in.getAccessor().getStartEnd((index));
      out3.start = ((int) startEnd);
      out3.end = ((int) (startEnd >> 32));
    }
    if (!(out3.isSet == 0)) {
      ((NullableVarCharVector) out).getMutator()
                                   .setSafe((outdex), out3.isSet, out3.start, out3.end,
                                     out3.buffer);
    }
  }

  private void copyBigInt(VectorWrapper<?> wrapper, ValueVector out, int inIndex, int outdex) {
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

  private void copyBit(VectorWrapper<?> wrapper, ValueVector out, int inIndex, int outdex) {
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
