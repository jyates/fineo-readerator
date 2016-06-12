package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.rel.recombinator.physical.Recombinator;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.StoreClerk;
import org.apache.avro.Schema;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.read.drill.exec.store.rel.recombinator.physical.batch.FieldTransferMapper
  .UNKNOWN_FIELDS_MAP_ALIASES;

/**
 * Do the actual work of transforming input records to the expected customer type
 */
public class RecombinatorRecordBatch extends AbstractSingleRecordBatch<Recombinator> {

  private final StoreClerk.Metric metric;
  private Schema metricSchema;
  private boolean builtSchema;
  private FieldTransferMapper transferMapper;
  private List<TransferPair> transfers;
  private List<ValueVector> vectors = new ArrayList<>();
  private List<SingleMapWriter> writers = new ArrayList<>();

  protected RecombinatorRecordBatch(final Recombinator popConfig, final FragmentContext context,
    final RecordBatch incoming) throws
    OutOfMemoryException {
    super(popConfig, context, incoming);
    // parse out the things we actually care about
    Metric metric = popConfig.getMetricObj();
    this.metric = new StoreClerk.Metric(null, metric, null);
    this.transferMapper = new FieldTransferMapper();
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
      // build the resulting schema
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      this.builtSchema = true;
    }

    this.transfers = this.transferMapper.prepareTransfers(incoming, writers);

    // TODO change the output type when the underlying schema fields change

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
    List<FieldEntry> entries = getRawFieldNames();
    // IF we let the recombinator prel row type = input types, we would be obligated to produce
    // the dynamic prefixed fields (ie. T0¦¦companykey). They would then be removed by an upstream
    // Project(T0¦¦*=[$0]) (from StarConverter). However, now that we are specifying the fields
    // exactly, the upstream Project is merely (*=[$0]), so we only want to send the "approved"
    // fields - the required ones, unknown map, and the per-tenant fields.
    // We track the row type explicitly because the parent filter (on company and metric) needs to
    // have explicit fields positions matching what we told it originally in the physical plan. From
    // there it builds the actual mapping in the execution, so we don't need to keep the same order
    // here.
    addFields(entries);

    // necessary so we map fields with their actual name, not with the dyn. projected field name
    this.transferMapper.setMapFieldPrefixToStrip(prefix);
  }

  private void addFields(List<FieldEntry> entries) {
    for (FieldEntry field : entries) {
      TypeProtos.MajorType type = field.getType();
      List<String> aliases = field.getAliasNames();
      String fieldName = field.getOutputName();
      MaterializedField mat = MaterializedField.create(fieldName, type);
      ValueVector v = container.addOrGet(mat, callBack);

      if (aliases != null) {
        // field should map to itself in cases where we store the client-visible field name
        aliases.add(fieldName);
      }
      this.transferMapper.addField(aliases, v);

      // track of all the non-map vectors. Map vectors are managed separately in the transferMapper
      if (!(v instanceof MapVector)) {
        vectors.add(v);
      }
    }
  }

  private List<FieldEntry> getRawFieldNames() throws SchemaChangeException {
    // build list of fields that we need to add
    List<FieldEntry> entries = new ArrayList<>();
    // required fields
    for (String field : FineoCommon.REQUIRED_FIELDS) {
      TypeProtos.MajorType type = Types.optional(TypeProtos.MinorType.VARCHAR);
      if (field.equals(AvroSchemaEncoder.TIMESTAMP_KEY)) {
        type = Types.optional(TypeProtos.MinorType.BIGINT);
      }
      entries.add(new FieldEntry(field, type));
    }

    // add a map type field for unknown columns
    TypeProtos.MajorType type = Types.required(TypeProtos.MinorType.MAP);
    entries.add(new FieldEntry(FineoCommon.MAP_FIELD, type, UNKNOWN_FIELDS_MAP_ALIASES));

    // we know that the first value in the alias map is actually the user visible name right now.
    for (StoreClerk.Field field : this.metric.getUserVisibleFields()) {
      entries.add(new FieldEntry(field.getName(), getType(field), field.getAliases()));
    }
    return entries;
  }

  public class FieldEntry {
    private String outputName;
    private List<String> aliasNames;
    private TypeProtos.MajorType type;

    public FieldEntry(String field, TypeProtos.MajorType type) {
      this(field, type, newArrayList(field));
    }

    public FieldEntry(String outputName, TypeProtos.MajorType type, List<String> aliasNames) {
      this.outputName = outputName;
      this.aliasNames = aliasNames;
      this.type = type;
    }

    public String getOutputName() {
      return outputName;
    }

    public List<String> getAliasNames() {
      return aliasNames;
    }

    public TypeProtos.MajorType getType() {
      return type;
    }

    @Override
    public String toString() {
      return "FieldEntry{" +
             "outputName='" + outputName + '\'' +
             ", type=" + type +
             ", aliasNames=" + aliasNames +
             '}';
    }
  }

  private static TypeProtos.MajorType getType(StoreClerk.Field field) throws SchemaChangeException {
    Schema.Type type = field.getType();
    switch (type) {
      case STRING:
        return Types.optional(TypeProtos.MinorType.VARCHAR);
      case BYTES:
        return Types.optional(TypeProtos.MinorType.VARBINARY);
      case INT:
        return Types.optional(TypeProtos.MinorType.INT);
      case LONG:
        return Types.optional(TypeProtos.MinorType.BIGINT);
      case FLOAT:
        return Types.optional(TypeProtos.MinorType.FLOAT4);
      case DOUBLE:
        return Types.optional(TypeProtos.MinorType.FLOAT8);
      case BOOLEAN:
        return Types.optional(TypeProtos.MinorType.BIT);
      default:
        throw new SchemaChangeException(
          "We don't know how to handle type: " + type + " for field: " + field.getName());
    }
  }

  @Override
  protected IterOutcome doWork() {
    int incomingRecordCount = incoming.getRecordCount();

    // we have to allocate space in all the vectors b/c they are nullable. Basically, just sets
    // up the 'null' bit in the vector. When we do the write, then we get more space
    for (ValueVector vector : vectors) {
      AllocationHelper.allocateNew(vector, 1);
    }

    for (int i = 0; i < incomingRecordCount; i++) {
      this.transferMapper.combine(i);
    }

    // claim ownership of all the underlying vectors
    for (TransferPair pair : transfers) {
      pair.transfer();
    }

    return IterOutcome.OK;
  }

  @Override
  public int getRecordCount() {
    return incoming.getRecordCount();
  }
}
