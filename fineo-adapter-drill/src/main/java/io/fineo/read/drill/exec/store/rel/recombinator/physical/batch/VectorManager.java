package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import io.fineo.read.drill.exec.store.FineoCommon;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;

import java.util.HashMap;
import java.util.Map;

public class VectorManager {

  private final OutputMutator mutator;
  private Map<String, VectorHolder> required = new HashMap<>();
  private Map<String, String> knownInputToOutput = new HashMap<>();
  private Map<String, VectorHolder> knownOutputToVector = new HashMap<>();


  private VectorHolder<MapVector> radio;
  private Map<String, VectorHolder> unknown = new HashMap<>();

  public VectorManager(OutputMutator mutator) {
    for (String field : FineoCommon.REQUIRED_FIELDS) {
      required.put(field, new VectorHolder());
    }
    this.mutator = mutator;
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

  public void addUnknownField(MaterializedField field, String outputName)
    throws SchemaChangeException {
    if (radio == null) {
      radio = new VectorHolder<>();
      MaterializedField fm = MaterializedField.create(FineoCommon.MAP_FIELD, MapVector.TYPE);
      radio.vector = mutator.addField(fm, MapVector.class);
    }

    VectorHolder holder = new VectorHolder();
    Class<? extends ValueVector> clazz = getVectorClass(field);
    holder.vector = radio.vector.addOrGet(outputName, field.getType(), clazz);
    unknown.put(field.getName(), holder);
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

  private class VectorHolder<T extends ValueVector> {
    private T vector;
  }

  private ValueVector getVector(MaterializedField field) throws SchemaChangeException {
    Class<? extends ValueVector> clazz = getVectorClass(field);
    return mutator.addField(field, clazz);
  }

  private Class<? extends ValueVector> getVectorClass(MaterializedField field) {
    return BasicTypeHelper.getValueVectorClass(field.getType().getMinorType(), field.getDataMode());
  }
}
