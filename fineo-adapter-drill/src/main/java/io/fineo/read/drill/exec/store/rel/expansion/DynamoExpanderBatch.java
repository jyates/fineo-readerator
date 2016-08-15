package io.fineo.read.drill.exec.store.rel.expansion;

import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.exec.store.rel.VectorUtils;
import io.fineo.read.drill.exec.store.rel.recombinator.physical.batch.impl.Mutator;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.RepeatedBitVector;
import org.apache.drill.exec.vector.RepeatedVarBinaryVector;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Expands dynamo records that are map fields into constituent fields based on a map{id -> value}
 * and set[id]. Relies on the fact that dynamo maps are always id -> value and that we only ever
 * store a single type of value in each field (so no UNION needed).
 */
public class DynamoExpanderBatch extends AbstractSingleRecordBatch<DynamoExpander> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoExpanderBatch.class);
  private final VectorContainerWriter writer;
  private Map<String, ValueVector> vectorMap = new HashMap<>();
  private Map<String, MaterializedField> fieldMap = new HashMap<>();
  private final Mutator mutator;
  private int recordCount;

  public DynamoExpanderBatch(DynamoExpander expander, FragmentContext context,
    RecordBatch onlyElement) {
    super(expander, context, onlyElement);
    this.mutator = new Mutator(upstreamFieldName -> upstreamFieldName,
      oContext, callBack, container, vectorMap);
    this.writer = new VectorContainerWriter(mutator, false);
  }

  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    vectorMap.clear();
    // use the actual incoming vectors to determine schema
    for (VectorWrapper wrapper : incoming) {
      if (skip(wrapper)) {
        continue;
      }
      MaterializedField f = wrapper.getField();
      TypeProtos.MajorType type = getMajorType(wrapper);
      // for now its easier to just use copies. However, we should be able to switch to sv2/sv4
      MaterializedField field = MaterializedField.create(f.getName(), type);
      setVector(field);
    }
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return true;
  }

  @Override
  protected IterOutcome doWork() {
    List<List<Object>> rows = getRowRecordCount(incoming.getRecordCount());
    this.recordCount = rows.stream().mapToInt(list -> list.size()).sum();
    writer.reset();

    int rowCount = 0;
    for (List<Object> rowIds : rows) {
      int idCount = 0;
      for (Object idObj : rowIds) {
        for (VectorWrapper wrapper : incoming) {
          if (skip(wrapper)) {
            continue;
          }
          MaterializedField field = wrapper.getField();
          // expand the 'regular' fields to match the ids
          if (field.getType().getMinorType() != TypeProtos.MinorType.MAP) {
            VectorUtils.write(field.getName(), wrapper, writer.rootAsMap(), rowCount, idCount);
          } else {
            // extract the map value, if its present
            MapVector vector = (MapVector) wrapper.getValueVector();
            String name = (String) idObj;
            ValueVector out = vectorMap.get(name);
            ValueVector in = vector.getChild(name, out.getClass());
            VectorUtils.write(name, in, fieldMap.get(name).getType().getMinorType(),
              writer.rootAsMap(), rowCount, idCount);
          }
        }
        idCount++;
      }
      rowCount++;
    }


    return IterOutcome.OK;
  }

  /**
   * Skip the ID field
   */
  private boolean skip(VectorWrapper wrapper) {
    MaterializedField field = wrapper.getField();
    return field.getName().equals(Schema.ID_FIELD);
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  private List<List<Object>> getRowRecordCount(int recordCount) {
    List<List<Object>> ids = new ArrayList<>();
    for (VectorWrapper wrapper : this.incoming) {
      MaterializedField field = wrapper.getField();
      String name = field.getName();
      if (!name.equals(Schema.ID_FIELD)) {
        continue;
      }
      // count the number of ids for each row to get a total number of rows
      for (int i = 0; i < recordCount; i++) {
        List<Object> row = (List<Object>) wrapper.getValueVector().getAccessor().getObject(i);
        ids.add(row);
      }
      break;
    }
    return ids;
  }

  private ValueVector setVector(MaterializedField field) {
    String name = field.getName();
    ValueVector v = vectorMap.get(name);
    if (v == null) {
      v = TypeHelper.getNewVector(field, oContext.getAllocator(), callBack);
      fieldMap.put(name, field);
      container.add(v);
    }
    return v;
  }

  private TypeProtos.MajorType getMajorType(VectorWrapper wrapper) {
    TypeProtos.MinorType type = wrapper.getField().getType().getMinorType();
    if (type != TypeProtos.MinorType.MAP) {
      return wrapper.getField().getType();
    }

    MapVector map = (MapVector) wrapper.getValueVector();
    ValueVector item = map.iterator().next();
    // bit
    if (item instanceof NullableBitVector) {
      return Types.optional(TypeProtos.MinorType.BIT);
    } else if (item instanceof RepeatedBitVector) {
      return Types.repeated(TypeProtos.MinorType.BIT);
    } else if (item instanceof BitVector) {
      return Types.required(TypeProtos.MinorType.BIT);
    }
    // string
    else if (item instanceof NullableVarCharVector) {
      return Types.optional(TypeProtos.MinorType.VARCHAR);
    } else if (item instanceof RepeatedVarCharVector) {
      return Types.repeated(TypeProtos.MinorType.VARCHAR);
    } else if (item instanceof VarCharVector) {
      return Types.required(TypeProtos.MinorType.VARCHAR);
    }
    // binary
    else if (item instanceof NullableVarBinaryVector) {
      return Types.optional(TypeProtos.MinorType.VARBINARY);
    } else if (item instanceof RepeatedVarBinaryVector) {
      return Types.repeated(TypeProtos.MinorType.VARBINARY);
    } else if (item instanceof VarBinaryVector) {
      return Types.required(TypeProtos.MinorType.VARBINARY);
    }
    throw new IllegalArgumentException("Cannot support map vector of types: " + item);
  }

  private Class<? extends ValueVector> getVectorClass(MaterializedField field) {
    return BasicTypeHelper.getValueVectorClass(field.getType().getMinorType(), field.getDataMode());
  }
}
