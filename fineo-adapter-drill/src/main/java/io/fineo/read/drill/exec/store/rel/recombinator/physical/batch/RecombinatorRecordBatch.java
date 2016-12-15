package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.rel.VectorUtils;
import io.fineo.read.drill.exec.store.rel.recombinator.physical.Recombinator;
import io.fineo.read.drill.exec.store.rel.recombinator.physical.batch.impl.AliasFieldNameManager;
import io.fineo.read.drill.exec.store.rel.recombinator.physical.batch.impl.Mutator;
import io.fineo.read.drill.exec.store.rel.recombinator.physical.batch.impl.VectorManager;
import io.fineo.schema.FineoStopWords;
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
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
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
  protected final VectorContainerWriter writer;

  private final AliasFieldNameManager aliasMap;
  protected final VectorManager vectors;
  private final boolean radio;
  private boolean builtSchema;

  protected final Mutator mutator;
  private String prefix;

  protected RecombinatorRecordBatch(final Recombinator popConfig, final FragmentContext context,
    final RecordBatch incoming) throws
    OutOfMemoryException {
    super(popConfig, context, incoming);
    // figure out how we should map the fields
    Metric metric = popConfig.getMetricObj();
    StoreClerk.Metric clerkMetric = StoreClerk.Metric.metricOnlyFunctions(metric);
    OptionManager options = context.getOptions();
    String partitionDesignator =
      options.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
    this.radio = FineoCommon.isRadioEnabled();
    this.aliasMap = new AliasFieldNameManager(clerkMetric, partitionDesignator, radio);
    // handle doing the vector allocation
    this.vectors = new VectorManager(aliasMap, oContext, callBack, container);
    // mutator wrapper around the container for managing vectors
    this.mutator = vectors.getMutator();
    // do the writing for fields that we need to copy piecemeal
    this.writer = new VectorContainerWriter(mutator, false);
  }

  /**
   * Generally, the schema is fixed after the first call. However, when the upstream schema changes
   * we have to map a new incoming field to one of the outgoing fields.
   *
   * @return <tt>true</tt> if the schema we return has changed
   */
  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    return createSchema(this.builtSchema);
  }

  protected boolean createSchema(boolean hadSchema) throws SchemaChangeException {
    return createSchema(hadSchema, true);
  }

  protected boolean createSchema(boolean hadSchema, boolean optionallyRefreshRadioVector) throws
    SchemaChangeException {
    if (!hadSchema) {
      container.clear();
    }
    // figure out what the table prefix is from the sub-table
    BatchSchema inschema = incoming.getSchema();
    String prefix = null;
    for (MaterializedField f : inschema) {
      String name = f.getName();
      if (FineoStopWords.DRILL_STAR_PREFIX_PATTERN.matcher(name).matches()) {
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

    if (radio) {
      // always create the radio field - necessary to avoid casting conflicts when using multiple
      // sources, only some of which have a unknown fields see
      // UnionAllRecordBatch$UnionAllInput#inferOutputFields
      vectors.ensureRadio();
    }

    // handle creating the other fields from the incoming schema; things like the alias and
    // unknown fields
    for (MaterializedField field : inschema) {
      String name = field.getName();
      boolean dynamic = name.startsWith(prefix);
      String outputName = stripDynamicProjectPrefix(name);
      // this is a dynamic field with an alias that we know about
      if (aliasMap.shouldSkip(outputName, dynamic)) {
        LOG.trace("Skipping field {} => {}", name, outputName);
        continue;
      }

      // only radio supports dynamic fields
      if (radio) {
        // its an unknown field - we aren't skipping it and its a dynamic field
        if (dynamic) {
          boolean isNewVector = vectors.addUnknownField(field, outputName);
          // this is an 'update' to the schema
          if (hadSchema && isNewVector && optionallyRefreshRadioVector) {
            // "replace" the map so we get a new schema generated for the map
            MapVector radio = vectors.clearRadio();
            if (radio != null) {
              container.remove(radio);
            }
            // rebuild the schema, but this time we will add back in the unknown fields into a new
            // map vector. This ensures that we create a new schema with new MaterializedFields and
            // don't muddy the upstream operator with changing fields in a schema. If we didn't do
            // this, we would have a schema change (marked from the deep schema callback) but the
            // schema we passed up would already have been changed b/c the Map's MF would have
            // added the child and thus we would get a case where schema.equals(newSchema), but
            // there was a schema change. Specifically, see ExternalSortBatch#innerNext
            return createSchema(true, false);
          }
          continue;
        }
      }

      outputName = aliasMap.getOutputName(outputName);
      if (outputName == null) {
        LOG.trace("Skipping: '{}' because not dynamic field, but no alias mapping found", name);
        continue;
      }
      // handles alias mapping and required fields
      vectors.addKnownField(field, outputName);
    }

    this.builtSchema = true;
    if (mutator.isNewSchema()) {
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      return true;
    }
    return false;
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
        LOG.trace("Adding transfer pair for {}", name);
        transfers.add(wrapper.getValueVector().makeTransferPair(out));
        continue;
      }

      out = vectors.getKnownField(name);
      if (out == null) {
        LOG.trace("No output vector found - skipping field {} ", name);
        continue;
      }

      // #startup - we can be even smarter and not copy the fields at all if all the values are
      // set for all positions in the vector. Even even smarter would be figuring out how to
      // slice parts of a vector and then transferring the non-null bits from each alias
      LOG.trace("Manually copying fields for {}", name);
      for (int i = 0; i < incomingRecordCount; i++) {
        // only write non-null values
        if (!wrapper.getValueVector().getAccessor().isNull(i)) {
          LOG.trace("{}) Copying field from {} because not null", i, name);
          VectorUtils.write(name, wrapper, this.writer.rootAsMap(), i);
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
