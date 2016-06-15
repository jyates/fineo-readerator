package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import com.amazonaws.util.Base64;
import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.rel.recombinator.physical.Recombinator;
import io.fineo.schema.store.StoreClerk;
import io.netty.buffer.DrillBuf;
import org.apache.avro.Schema;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Do the actual work of transforming input records to the expected customer type
 */
public class RecombinatorRecordBatch extends AbstractSingleRecordBatch<Recombinator> {
  private static final Logger LOG = LoggerFactory.getLogger(RecombinatorRecordBatch.class);

  private final StoreClerk.Metric metric;
  private final Map<String, String> aliasMap;
  private final VectorContainerWriter writer;
  private Schema metricSchema;
  private boolean builtSchema;
  private FieldTransferMapper transferMapper;
  private List<TransferPair> transfers;
  private List<ValueVector> vectors = new ArrayList<>();
  private Map<String, ValueVector> fieldVectorMap = new HashMap<>();
  private final Mutator mutator = new Mutator();
  private String prefix;

  protected RecombinatorRecordBatch(final Recombinator popConfig, final FragmentContext context,
    final RecordBatch incoming) throws
    OutOfMemoryException {
    super(popConfig, context, incoming);
    // parse out the things we actually care about
    Metric metric = popConfig.getMetricObj();
    this.metric = new StoreClerk.Metric(null, metric, null);
    this.transferMapper = new FieldTransferMapper();
    this.aliasMap = getFieldMap();
    this.writer = new VectorContainerWriter(mutator, false);
  }

  private Map<String, String> getFieldMap() {
    Map<String, String> map = new HashMap<>();
    // build list of fields that we need to add
    // required fields
    for (String field : FineoCommon.REQUIRED_FIELDS) {
      map.put(field, field);
    }

    map.put(FineoCommon.MAP_FIELD, FineoCommon.MAP_FIELD);

    for (StoreClerk.Field field : this.metric.getUserVisibleFields()) {
      map.put(field.getName(), field.getName());
      for (String alias : field.getAliases()) {
        map.put(alias, field.getName());
      }
    }
    return map;
  }

  /**
   * Generally, the schema is fixed after the first call. However, when the upstream schema changes
   * we have to map a new incoming field to one of the outgoing fields.
   *
   * @return <tt>true</tt> if the schema we return has changed, which only should ever happen on
   * the first call to this method
   */
  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    boolean hadSchema = this.builtSchema;
    if (!builtSchema) {
      // buildSchema() is only used the first time in AbstractBatchRecord and only when creating
      createSchema();
      this.builtSchema = true;
    }

//    this.transferMapper.prepareTransfers(incoming);

    return hadSchema ^ builtSchema;
  }

  protected void createSchema() throws SchemaChangeException {
    container.clear();
    // figure out what the table prefix is from the sub-table
    BatchSchema inschema = incoming.getSchema();
    String prefix = null;
    for (MaterializedField f : inschema) {
      String name = f.getName();
      if (name.contains(StarColumnHelper.PREFIX_DELIMITER)) {
        prefix = name.substring(0, name.indexOf(StarColumnHelper.PREFIX_DELIMITER));
        break;
      }
    }
    Preconditions.checkArgument(prefix != null,
      "No dynamic table prefix (column starting with TXX%s) from incoming schema: %s",
      StarColumnHelper.PREFIX_DELIMITER, inschema);

    prefix = prefix + StarColumnHelper.PREFIX_DELIMITER;
    // IF we let the recombinator prel row type = input types, we would be obligated to produce
    // the dynamic prefixed fields (ie. T0¦¦companykey). They would then be removed by an upstream
    // Project(T0¦¦*=[$0]) (from StarConverter). However, now that we are specifying the fields
    // exactly, the upstream Project is merely (*=[$0]), so we only want to send the "approved"
    // fields - the required ones, unknown map, and the per-tenant fields.
    // We track the row type explicitly because the parent filter (on company and metric) needs to
    // have explicit fields positions matching what we told it originally in the physical plan. From
    // there it builds the actual mapping in the execution, so we don't need to keep the same order
    this.prefix = prefix;
  }

  @Override
  protected IterOutcome doWork() {
    int incomingRecordCount = incoming.getRecordCount();
    writer.allocate();
    writer.reset();

    for (int i = 0; i < incomingRecordCount; i++) {
      writer.setPosition(i);
      for (VectorWrapper wrapper : this.incoming) {
        BaseWriter.MapWriter currentWriter = this.writer.rootAsMap();
        String name = wrapper.getField().getName();
        // dynamic fields, i.e. T0¦¦ are only added if they do not have a known alias name. This
        // ensures that we only handle the casted types, not the ANY typed fields
        boolean dynamic = name.startsWith(prefix);
        String outputName = stripDynamicProjectPrefix(name);
        if (dynamic) {
          // this is a dynamic field with an alias that we know about
          if (!outputName.equals(FineoCommon.MAP_FIELD) && getOutputFieldName(outputName) != null) {
            LOG.debug("Skipping field {} => {}", name, outputName);
            continue;
          }
          currentWriter = currentWriter.map(FineoCommon.MAP_FIELD);
        } else {
          outputName = aliasMap.get(outputName);
        }
        write(outputName, wrapper, currentWriter);
      }
    }

    if (mutator.isNewSchema()) {
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      return IterOutcome.OK_NEW_SCHEMA;
    }

    return IterOutcome.OK;
  }

  private void write(String outputName, VectorWrapper wrapper, BaseWriter.MapWriter writer) {
    ValueVector vector = wrapper.getValueVector();
    FieldReader reader = vector.getReader();
    if (!reader.isSet()) {
      LOG.debug("No field value set for {} => {}", outputName, wrapper.getField());
      return;
    }
    LOG.trace("Mapping {} => {}", wrapper.getField(), outputName);
    switch (wrapper.getField().getType().getMinorType()) {
      case VARCHAR:
      case FIXEDCHAR:
        reader.copyAsValue(writer.varChar(outputName));
        break;
      case FLOAT4:
        reader.copyAsValue(writer.float4(outputName));
        break;
      case FLOAT8:
        reader.copyAsValue(writer.float8(outputName));
        break;
      case VAR16CHAR:
      case FIXED16CHAR:
        reader.copyAsValue(writer.var16Char(outputName));
        break;
      case INT:
        reader.copyAsValue(writer.integer(outputName));
        break;
      case SMALLINT:
        reader.copyAsValue(writer.smallInt(outputName));
        break;
      case TINYINT:
        reader.copyAsValue(writer.tinyInt(outputName));
        break;
      case DECIMAL9:
        reader.copyAsValue(writer.decimal9(outputName));
        break;
      case DECIMAL18:
        reader.copyAsValue(writer.decimal18(outputName));
        break;
      case UINT1:
        reader.copyAsValue(writer.uInt1(outputName));
        break;
      case UINT2:
        reader.copyAsValue(writer.uInt2(outputName));
        break;
      case UINT4:
        reader.copyAsValue(writer.uInt4(outputName));
        break;
      case UINT8:
        reader.copyAsValue(writer.uInt8(outputName));
        break;
      case BIGINT:
        reader.copyAsValue(writer.bigInt(outputName));
        break;
      case BIT:
        reader.copyAsValue(writer.bit(outputName));
        break;
      case VARBINARY:
      case FIXEDBINARY:
        reader.copyAsValue(writer.varBinary(outputName));
        break;
      default:
        throw new UnsupportedOperationException("Cannot convert field: " + wrapper);
    }
  }


  private byte[] decode(byte[] input) {
    return Base64.decode(input);
  }

  @Override
  public int getRecordCount() {
    return incoming.getRecordCount();
  }

  private String getOutputFieldName(String name) {
    return this.aliasMap.get(name);
  }

  private String stripDynamicProjectPrefix(String name) {
    if (name.startsWith(prefix)) {
      name = name.substring(prefix.length());
    }
    return name;
  }

  private class Mutator implements OutputMutator {
    /**
     * Whether schema has changed since last inquiry (via #isNewSchema}).  Is
     * true before first inquiry.
     */
    private boolean schemaChanged = true;

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T addField(MaterializedField field,
      Class<T> clazz) throws SchemaChangeException {
      String name = field.getName();
      boolean dynamic = name.startsWith(prefix);
      if (dynamic) {
        String stripped = stripDynamicProjectPrefix(name);
        assert getOutputFieldName(stripped) == null :
          "Got an output field: " + getOutputFieldName(stripped) +
          " for input: " + name + " => " + stripped + ", but this field should have been ignored!";
        // get the mapvector for the radio
        MapVector map = container.addOrGet(field, callBack);
        return null;
      }

      String outputName = getOutputFieldName(name);
      Preconditions.checkNotNull(outputName,
        "Didn't find an output name for: %s, it should be handled as a dynamic _fm field!", name);
      // Check if the field exists.
      ValueVector v = fieldVectorMap.get(field.getName());
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
  }
}
