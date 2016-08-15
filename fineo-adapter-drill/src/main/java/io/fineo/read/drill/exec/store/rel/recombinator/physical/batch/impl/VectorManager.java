package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch.impl;

import io.fineo.read.drill.exec.store.FineoCommon;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;

import java.util.HashMap;
import java.util.Map;

public class VectorManager {

  private final Mutator mutator;
  private Map<String, ValueVector> fieldVectorMap = new HashMap<>();
  private Map<String, VectorHolder> required = new HashMap<>();
  private Map<String, String> knownInputToOutput = new HashMap<>();
  private Map<String, VectorHolder> knownOutputToVector = new HashMap<>();

  private VectorHolder<MapVector> radio;
  private Map<String, VectorHolder> unknown = new HashMap<>();

  public VectorManager(AliasFieldNameManager aliasMap, OperatorContext oContext,
    SchemaChangeCallBack callBack, VectorContainer container) {
    for (String field : FineoCommon.REQUIRED_FIELDS) {
      required.put(field, new VectorHolder());
    }
    this.mutator = new Mutator(aliasMap, oContext, callBack, container, fieldVectorMap);
  }

  public void addKnownField(MaterializedField field, String outputName)
    throws SchemaChangeException {
    if (required.containsKey(outputName)) {
      VectorHolder holder = required.get(outputName);
      holder.vector = getVector(field.withPath(outputName));
    } else {
      String input = field.getName();
      String existingOut = knownInputToOutput.get(input);
      if (existingOut == null) {
        knownInputToOutput.put(input, outputName);
      }
      VectorHolder holder = knownOutputToVector.get(outputName);
      if (holder == null) {
        holder = new VectorHolder();
        holder.vector = getVector(field.withPath(outputName));
        knownOutputToVector.put(field.getName(), holder);
      }
    }
  }

  public void ensureRadio() throws SchemaChangeException {
    if (radio != null) {
      return;
    }
    radio = new VectorHolder<>();
    MaterializedField fm = MaterializedField.create(FineoCommon.MAP_FIELD, MapVector.TYPE);
    radio.vector = mutator.addField(fm, MapVector.class);
  }

  public MapVector clearRadio() {
    if (this.radio == null || this.radio.vector == null) {
      return null;
    }
    // remove references to the vector and the holder
    this.radio.vector.clear();
    ValueVector map = this.fieldVectorMap.remove(FineoCommon.MAP_FIELD);
    this.radio = null;
    return (MapVector) map;
  }

  /**
   * Add an unknown field vector
   *
   * @param field      description of the field to add
   * @param outputName the name out of the vector in the output container
   * @return <tt>true</tt> if this was a new vector, <tt>false</tt> otherwise
   * @throws SchemaChangeException
   */
  public boolean addUnknownField(MaterializedField field, String outputName)
    throws SchemaChangeException {
    VectorHolder holder = new VectorHolder();
    Class<? extends ValueVector> clazz = getVectorClass(field);
    ValueVector prev = radio.vector.getChild(outputName);
    holder.vector = radio.vector.addOrGet(outputName, field.getType(), clazz);
    unknown.put(field.getName(), holder);
    return prev == null || prev != holder.vector;
  }

  public ValueVector getRequiredVector(String name) {
    return getVectorFrom(required, name);
  }

  public ValueVector getUnknownFieldVector(String name) {
    return getVectorFrom(unknown, name);
  }

  public ValueVector getKnownField(String name) {
    String output = knownInputToOutput.get(name);
    return getVectorFrom(knownOutputToVector, output);
  }

  private static ValueVector getVectorFrom(Map<String, VectorHolder> map, String name) {
    ValueVector vv = null;
    VectorHolder holder = map.get(name);
    if (holder != null) {
      vv = holder.vector;
    }
    return vv;
  }

  public Mutator getMutator() {
    return mutator;
  }

  private class VectorHolder<T extends ValueVector> {
    private T vector;
  }

  private ValueVector getVector(MaterializedField field) throws SchemaChangeException {
    ValueVector out = fieldVectorMap.get(field.getName());
    if (out == null) {
      Class<? extends ValueVector> clazz = getVectorClass(field);
      out = mutator.addField(field, clazz);
    }
    return out;
  }

  private Class<? extends ValueVector> getVectorClass(MaterializedField field) {
    return BasicTypeHelper.getValueVectorClass(field.getType().getMinorType(), field.getDataMode());
  }
}
