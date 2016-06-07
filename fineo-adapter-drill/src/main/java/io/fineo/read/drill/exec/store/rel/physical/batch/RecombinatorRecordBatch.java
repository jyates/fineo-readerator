package io.fineo.read.drill.exec.store.rel.physical.batch;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.rel.physical.Recombinator;
import io.fineo.schema.avro.AvroSchemaEncoder;
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
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.avro.AvroSchemaEncoder.BASE_FIELDS_KEY;

/**
 * Do the actual work of transforming input records to the expected customer type
 */
public class RecombinatorRecordBatch extends AbstractSingleRecordBatch<Recombinator> {

  private static final String CATCH_ALL_MAP = null;
  private final Map<String, List<String>> cnameToAlias;
  private Schema metricSchema;
  private boolean builtSchema;
  private FieldTransferMapper transferMapper;
  private List<TransferPair> transfers;
  private List<ValueVector> vectors = new ArrayList<>();
  private List<BaseWriter.ComplexWriter> writers = new ArrayList<>();

  protected RecombinatorRecordBatch(final Recombinator popConfig, final FragmentContext context,
    final RecordBatch incoming) throws
    OutOfMemoryException {
    super(popConfig, context, incoming);
    // parse out the things we actually care about
    Metric metric = popConfig.getMetricObj();
    this.cnameToAlias = metric.getMetadata().getCanonicalNamesToAliases();
    this.metricSchema = new Schema.Parser().parse(metric.getMetricSchema());
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

    this.transfers = this.transferMapper.prepareTransfers(incoming, vectors, writers);

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

    List<FieldEntry> entries = getRawFieldNames();
    // we have to add each entry with and without the dynamic prefix so it gets unwrapped later by
    // the Project inserted by the StarColumnConverter, but also keep the "original" output field
    // names so we can match them to downstream filters which still use the simple names, rather
    // than input field references
    prefix += StarColumnHelper.PREFIX_DELIMITER;
    addFields(entries, "");
    addFields(entries, prefix);

    // necessary so we map fields with their actual name, not with the dyn. projected field name
    this.transferMapper.setMapFieldPrefixtoStrip(prefix);
  }

  private void addFields(List<FieldEntry> entries, String prefix) {
    for (FieldEntry field : entries) {
      TypeProtos.MajorType type = field.getType();
      List<String> aliases = field.getAliasNames();
      String fieldName = prefix + field.getOutputName();
      MaterializedField mat = MaterializedField.create(fieldName, type);
      ValueVector v = container.addOrGet(mat, callBack);

      // aliases need to also be converted to the prefix
      if (aliases != null) {
        aliases = aliases.stream().map(alias -> prefix + alias).collect(Collectors.toList());
        // field should map to itself in cases where we store the client-visible field name
        aliases.add(fieldName);
      }
      this.transferMapper.addField(aliases, v);
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
    TypeProtos.MajorType type = Types.optional(TypeProtos.MinorType.MAP);
    entries.add(new FieldEntry(FineoCommon.MAP_FIELD, type, false));

    // we know that the first value in the alias map is actually the user visible name right now.
    for (Map.Entry<String, List<String>> entry : cnameToAlias.entrySet()) {
      String cname = entry.getKey();
      if (cname.equals(BASE_FIELDS_KEY)) {
        continue;
      }
      String alias = entry.getValue().get(0);
      entries.add(new FieldEntry(alias, getFieldType(cname), entry.getValue()));
    }
    return entries;
  }

  public class FieldEntry {
    private String outputName;
    private List<String> aliasName;
    private TypeProtos.MajorType type;

    public FieldEntry(String field, TypeProtos.MajorType type) {
      this(field, type, true);
    }

    public FieldEntry(String field, TypeProtos.MajorType type, boolean hasAlias) {
      this(field, type, hasAlias ? newArrayList(field) : null);
    }

    public FieldEntry(String outputName, TypeProtos.MajorType type, List<String> aliasNames) {
      this.outputName = outputName;
      this.aliasName = aliasNames;
      this.type = type;
    }

    public String getOutputName() {
      return outputName;
    }

    public List<String> getAliasNames() {
      return aliasName;
    }

    public TypeProtos.MajorType getType() {
      return type;
    }
  }

  private TypeProtos.MajorType getFieldType(String cname) throws SchemaChangeException {
    // this is pretty ugly that we just leave this out here. It really should be better encapsulated
    // in the schema project... but we can do that later. #startup
    Schema.Field field = this.metricSchema.getField(cname);
    Schema.Type type = field.schema().getField("value").schema().getType();
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
          "We don't know how to handle type: " + type + " for field: " + cname + "->" + cnameToAlias
            .get(cname));
    }
  }

  @Override
  protected IterOutcome doWork() {
    int incomingRecordCount = incoming.getRecordCount();

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
