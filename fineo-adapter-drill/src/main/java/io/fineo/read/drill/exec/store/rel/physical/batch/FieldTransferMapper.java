package io.fineo.read.drill.exec.store.rel.physical.batch;

import com.google.common.base.Preconditions;
import io.fineo.schema.Pair;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Combines multiple potential sub-fields into a single know field
 */
public class FieldTransferMapper {

  private Map<String, ValueVector> fieldMapping = new HashMap<>();
  private List<Pair<VectorWrapper, VectorOrWriter>> inOutMapping = new ArrayList<>();
  private String mapFieldPrefixtoStrip;

  public void setMapFieldPrefixtoStrip(String mapFieldPrefixtoStrip) {
    this.mapFieldPrefixtoStrip = mapFieldPrefixtoStrip;
  }

  /**
   * Do the actual work of mapping the incoming records to the outgoing vectors
   *
   * @param incomingIndex
   */
  public void combine(int incomingIndex) {
    for (Pair<VectorWrapper, VectorOrWriter> p : inOutMapping) {
      copy(p.getKey(), p.getValue(), incomingIndex, 0);
    }
  }

  /**
   * Finds all the matching output vectors and makes a new transfer pair for the incoming vectors
   * based on the name of the field (assumed to be unique from the underlying projection wrapper
   * around the scan
   *
   * @return list of ownership inOutMapping for the vectors in the group
   */
  public List<TransferPair> prepareTransfers(RecordBatch in,
    List<SingleMapWriter> writers) {
    this.inOutMapping.clear();
    List<TransferPair> transferPairs = new ArrayList<>();
    for (VectorWrapper<?> wrapper : in) {
      MaterializedField field = wrapper.getField();
      String name = field.getName();
      ValueVector out = getOutput(name);

      //TODO this needs to also project for the non-prefix map field, which means we need a
      // multi-map OR acustom impl just for the map vector, which, admittedly, is a little bit
      // weird in contextwith the rest of the simple vector transfers

      // its an unknown field, we need to create a sub-vector for this field inside the map vector
      if (out instanceof MapVector) {
        MapVector mv = (MapVector) out;
        SingleMapWriter writer;
        if (writers.size() == 0) {
          writer = new SingleMapWriter(mv, null, true);
          writers.add(writer);
        } else {
          writer = writers.get(0);

        }
        this.inOutMapping.add(new Pair<>(wrapper, new VectorOrWriter(writer)));

        // each time we will need to allocate a new field in the map, which only works after the
        // mapRoot has been created
        writer.allocate();

      } else {
        this.inOutMapping.add(new Pair<>(wrapper, new VectorOrWriter(out)));

        // we just do a simple transfer for this vector
        transferPairs.add(wrapper.getValueVector().makeTransferPair(out));
      }
    }
    return transferPairs;
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

  /**
   *  -------------------------------------------------------------------------------------------
   *  TODO replace everything below this with generated code #startup
   *  -------------------------------------------------------------------------------------------
   */

  /**
   * Copy the value from the source to the target state
   */
  private void copy(VectorWrapper<?> wrapper, VectorOrWriter out, int inIndex, int outIndex) {
    if (out.hasVector()) {
      copyVector(wrapper, out.vv, inIndex, outIndex);
    } else {
      copyMapField(wrapper, out.map, inIndex);
    }
  }

  /**
   * Transfer from the wrapper into a field in the map. This also has the side-effect of creating a
   * field vector in the container, which we can reference later to create the transfer pair.
   */
  private void copyMapField(VectorWrapper<?> wrapper, BaseWriter.MapWriter map, int inIndex) {
    // make sure that we are readying a real value
    ValueVector in = wrapper.getValueVector();
    Preconditions
      .checkArgument(!in.getAccessor().isNull(inIndex), "Want to map a field that is set to null!");

    // switch for the type... probably better to do this as gen code...
    MaterializedField field = wrapper.getField();
    String name = field.getName();
    assert name.startsWith(mapFieldPrefixtoStrip) :
      "Field is unknown (mapped), but not a dyn. projected field";
    name = name.substring(mapFieldPrefixtoStrip.length());

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

  private void copyVector(VectorWrapper<?> wrapper, ValueVector out, int inIndex, int outIndex) {
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
      default:
        throw new UnsupportedOperationException("Cannot convert field: " + field);
    }
  }

  private class VectorOrWriter {
    private ValueVector vv;
    private BaseWriter.MapWriter map;

    public VectorOrWriter(ValueVector out) {
      this.vv = out;
    }

    public VectorOrWriter(BaseWriter.MapWriter mapWriter) {
      this.map = mapWriter;
    }

    public boolean hasVector() {
      return this.vv != null;
    }
  }
}
