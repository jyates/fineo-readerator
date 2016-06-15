package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Combines multiple potential sub-fields into a single know field
 */
public class FieldTransferMapper {
  private static final Logger LOG = LoggerFactory.getLogger(FieldTransferMapper.class);

  public static final List<String> UNKNOWN_FIELDS_MAP_ALIASES = null;
  public static final String UNKNOWN_FIELDS_MAP = null;

  private Map<String, ValueVector> aliasToOutputVectorMapping = new HashMap<>();
  private Map<VectorWrapper, VectorHandler> inOutMapping = new HashMap<>();
  private String mapFieldPrefixToStrip;
  private SingleMapWriter mapWriter;

  public void setMapFieldPrefixToStrip(String mapFieldPrefixToStrip) {
    this.mapFieldPrefixToStrip = mapFieldPrefixToStrip;
  }

  /**
   * Finds all the matching output vectors and makes a new transfer pair for the incoming vectors
   * based on the name of the field (assumed to be unique from the underlying projection wrapper
   * around the scan
   *
   * @return list of ownership inOutMapping for the vectors in the group
   */
  public List<TransferPair> prepareTransfers(RecordBatch in) {
    this.inOutMapping.clear();
    Set<ValueVector> mapped = new HashSet<>();
    List<TransferPair> transferPairs = new ArrayList<>();
    for (VectorWrapper<?> wrapper : in) {
      MaterializedField field = wrapper.getField();
      String name = field.getName();
      boolean dynamicField = name.startsWith(mapFieldPrefixToStrip);
      String stripped = stripDynamicProjectPrefix(name);
      if (dynamicField) {
        // check to see if we have the output for that field
        ValueVector out = this.aliasToOutputVectorMapping.get(stripped);
        // dynamic field for which we don't already have a mapping, it goes in the generic map
        if (out == null) {
          this.inOutMapping.put(wrapper, getDynamicHandler());
        }
        // ignore fields that have a mapping because we don't handle them as dynamic, but instead
        // handle their correctly cast version, which has a fixed name
        continue;
      }

      ValueVector out = getOutput(name);
      VectorHandler handler = inOutMapping.get(wrapper.getValueVector());
      if(handler == null){
        handler = new AliasFieldVectorHandler(out);
      }
      this.inOutMapping.put(wrapper, handler);
      if (mapped.contains(out)) {
        LOG.debug(
          "Skipping mapping for {} because we already have a vector to handle that field", name);
        continue;
      }
    }
    return transferPairs;
  }

  private DynamicVectorHandler getDynamicHandler() {
    if (this.mapWriter == null) {
      MapVector vector = (MapVector) this.aliasToOutputVectorMapping.get(UNKNOWN_FIELDS_MAP);
      mapWriter = new SingleMapWriter(vector, null, true);
    }
    return new DynamicVectorHandler(mapWriter);
  }

  public void addField(String inputName, ValueVector output) {
    aliasToOutputVectorMapping.put(inputName, output);
  }

  public void addField(List<String> aliasNames, ValueVector vvOut) {
    if (aliasNames == UNKNOWN_FIELDS_MAP_ALIASES) {
      addField(UNKNOWN_FIELDS_MAP, vvOut);
      return;
    }
    for (String in : aliasNames) {
      addField(in, vvOut);
    }
  }

  private ValueVector getOutput(String incoming) {
    ValueVector value = this.aliasToOutputVectorMapping.get(incoming);
    // unknown field type
    if (value == null) {
      value = this.aliasToOutputVectorMapping.get(UNKNOWN_FIELDS_MAP);
    }
    return value;
  }

  /**
   * Do the actual work of mapping the incoming records to the outgoing vectors
   *
   * @param incomingIndex index into the incoming mapping to read. Basically, this is the row number
   */
  public void combine(int incomingIndex) {
    Set<ValueVector> pendingVectors = new HashSet<>();
    for (Map.Entry<VectorWrapper, VectorHandler> entry : inOutMapping.entrySet()) {
      String sanitized = stripDynamicProjectPrefix(entry.getKey().getField().getName());
      entry.getValue().copyField(entry.getKey(), incomingIndex, sanitized);
    }
  }

  /**
   *  -------------------------------------------------------------------------------------------
   *  TODO replace everything below this with generated code #startup
   *  -------------------------------------------------------------------------------------------
   */


  private void checkFieldNotSet(VectorWrapper<?> wrapper) {
    Preconditions.checkArgument(!wrapper.getValueVector().getReader().isSet(),
      "Incoming vector for: %s was set, but we already have a value for the output field!",
      wrapper.getField());
  }

  private String stripDynamicProjectPrefix(String name) {
    if (name.startsWith(mapFieldPrefixToStrip)) {
      name = name.substring(mapFieldPrefixToStrip.length());
    }
    return name;
  }


  /**
   * Transfer ownership of vector's buffers form the source to the output vectors, if possible
   */
  public void transferOwnership() {

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
