package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.exec.store.rel.recombinator.physical.Recombinator;
import io.fineo.schema.store.StoreClerk;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Do the actual work of transforming input records to the expected customer type
 */
public class RecombinatorRecordBatch extends AbstractSingleRecordBatch<Recombinator> {
  private static final Logger LOG = LoggerFactory.getLogger(RecombinatorRecordBatch.class);

  // Use a VectorContainerWriter + a custom mutator to make it easier to maange vectors, rather than
  // trying to manage them each independently.
  private final VectorContainerWriter writer;

  private final AliasFieldNameManager aliasMap;
  private final VectorManager vectors;
  private boolean builtSchema;

  private final Mutator mutator;
  private String prefix;

  protected RecombinatorRecordBatch(final Recombinator popConfig, final FragmentContext context,
    final RecordBatch incoming) throws
    OutOfMemoryException {
    super(popConfig, context, incoming);
    // figure out how we should map the fields
    Metric metric = popConfig.getMetricObj();
    StoreClerk.Metric clerk = new StoreClerk.Metric(null, metric, null);
    String partitionDesignator =
      context.getOptions().getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
    this.aliasMap = new AliasFieldNameManager(clerk, partitionDesignator);

    // handle doing the vector allocation
    this.mutator = new Mutator(aliasMap, oContext, callBack, container);
    // do the writing for fields that we need to copy piecemeal
    this.writer = new VectorContainerWriter(mutator, false);
    // manage which vector we should use for each field
    this.vectors = new VectorManager(mutator);
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

    for (MaterializedField field : inschema) {
      String name = field.getName();
      boolean dynamic = name.startsWith(prefix);
      String outputName = stripDynamicProjectPrefix(name);
      // this is a dynamic field with an alias that we know about
      if (aliasMap.shouldSkip(outputName, dynamic)) {
        LOG.debug("Skipping field {} => {}", name, outputName);
        continue;
      }

      // its an unknown field - we aren't skipping it and its a dynamic field
      if (dynamic) {
        vectors.addUnknownField(field, outputName);
      } else {
        outputName = aliasMap.getOutputName(outputName);
        vectors.addKnownField(field, outputName);
      }
    }

    if (mutator.isNewSchema()) {
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    }
  }

  @Override
  protected IterOutcome doWork() {
    int incomingRecordCount = incoming.getRecordCount();
    mutator.allocate(incomingRecordCount);
    writer.reset();

    List<TransferPair> transfers = new ArrayList<>();
    // create the transfer pairs for the ones we know about
    for (VectorWrapper wrapper : this.incoming) {
      MaterializedField field = wrapper.getField();
      String name = field.getName();
      ValueVector out = vectors.getRequiredVector(name);
      if (out == null) {
        out = vectors.getUnknownFieldVector(name);
      }

      if (out != null) {
        LOG.debug("Adding transfer pair for {}", name);
        transfers.add(wrapper.getValueVector().makeTransferPair(out));
        continue;
      }

      out = vectors.getKnownField(name);
      if (out == null) {
        LOG.debug("No output vector found - skipping field {} ", name);
        continue;
      }

      LOG.debug("Manually copying fields for {}", name);
      for (int i = 0; i < incomingRecordCount; i++) {
        writer.setPosition(i);
        // only write non-null values
        if (!wrapper.getValueVector().getAccessor().isNull(i)) {
          write(name, wrapper, this.writer.rootAsMap());
        }
      }
    }

    for (TransferPair tp : transfers) {
      tp.transfer();
    }
    mutator.setRecordCount(incomingRecordCount);

    // parent manages returning the schema change if there was a schema change from the child
    return IterOutcome.OK;
  }

  private void write(String outputName, VectorWrapper wrapper, BaseWriter.MapWriter writer) {
    ValueVector vector = wrapper.getValueVector();
    FieldReader reader = vector.getReader();
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

  @Override
  public int getRecordCount() {
    return incoming.getRecordCount();
  }

  private String stripDynamicProjectPrefix(String name) {
    if (name.startsWith(prefix)) {
      name = name.substring(prefix.length());
    }
    return name;
  }
}
