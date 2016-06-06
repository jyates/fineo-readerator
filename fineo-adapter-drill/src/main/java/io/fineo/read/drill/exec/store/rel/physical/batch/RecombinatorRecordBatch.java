package io.fineo.read.drill.exec.store.rel.physical.batch;

import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.rel.physical.Recombinator;
import io.fineo.schema.avro.AvroSchemaEncoder;
import org.apache.avro.Schema;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.ValueVector;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Do the actual work of transforming input records to the expected customer type
 */
public class RecombinatorRecordBatch extends AbstractSingleRecordBatch<Recombinator> {

  private final Map<String, List<String>> cnameToAlias;
  private Schema metricSchema;
  private boolean builtSchema;
  private BatchSchema previousSchema;
  private Combinator combinator;

  protected RecombinatorRecordBatch(final Recombinator popConfig, final FragmentContext context,
    final RecordBatch incoming) throws
    OutOfMemoryException {
    super(popConfig, context, incoming);
    // parse out the things we actually care about
    Metric metric = popConfig.getMetricObj();
    this.cnameToAlias = metric.getMetadata().getCanonicalNamesToAliases();
    this.metricSchema = new Schema.Parser().parse(metric.getMetricSchema());
    this.combinator = new Combinator();
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

    // add the new fields to the mapper
    BatchSchema inSchema = incoming.getSchema();
    combinator.updateSchema(inSchema);

    return hadSchema ^ builtSchema;
  }

  protected void createSchema() throws SchemaChangeException {
    container.clear();
    // required fields
    for (String field : FineoCommon.REQUIRED_FIELDS) {
      TypeProtos.MajorType type = Types.optional(TypeProtos.MinorType.VARCHAR);
      if (field.equals(AvroSchemaEncoder.TIMESTAMP_KEY)) {
        type = Types.optional(TypeProtos.MinorType.TIMESTAMP);
      }
      addField(field, field, type);
    }

    // add a map type field for unknown columns
    TypeProtos.MajorType type = Types.optional(TypeProtos.MinorType.MAP);
    addField((String) null, FineoCommon.MAP_FIELD, type);

    // we know that the first value in the alias map is actually the user visible name right now.
    for (Map.Entry<String, List<String>> entry : cnameToAlias.entrySet()) {
      String cname = entry.getKey();
      if (cname.equals("baseFields")) {
        continue;
      }
      String alias = entry.getValue().get(0);
      addField(entry.getValue(), alias, getFieldType(cname));
    }
  }

  private void addField(String alias, String field, TypeProtos.MajorType type) {
    List<String> aliases = alias == null ? null : newArrayList(alias);
    addField(aliases, field, type);
  }

  private void addField(List<String> aliases, String field, TypeProtos.MajorType type) {
    MaterializedField mat = MaterializedField.create(field, type);
    ValueVector v = TypeHelper.getNewVector(mat, oContext.getAllocator(), callBack);
    container.add(v);
    this.combinator.addField(aliases, field);
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
    this.combinator.combine(incomingRecordCount);
    return IterOutcome.OK;
  }

  @Override
  public int getRecordCount() {
    return incoming.getRecordCount();
  }
}
