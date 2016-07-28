package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.lambda.dynamo.Range;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import io.fineo.read.drill.exec.store.schema.FineoTable;
import io.fineo.schema.avro.AvroSchemaEncoder;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.util.ImmutableNullableList.of;
import static org.apache.drill.exec.planner.logical.DrillOptiq.toDrill;
import static org.apache.drill.exec.planner.physical.PrelUtil.getPlannerSettings;

/**
 * Rule that pushes a timerange filter (WHERE) past the recombinator and into the actual scan
 */
public abstract class ConvertFineoMarkerIntoFilteredInputTables extends RelOptRule {
  private static final Logger LOG =
    LoggerFactory.getLogger(ConvertFineoMarkerIntoFilteredInputTables.class);

  private static final String DYNAMO = "dynamo";
  private static final String DFS = "dfs";

  protected ConvertFineoMarkerIntoFilteredInputTables(RelOptRuleOperand operand, String name) {
    super(operand, name);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    FineoRecombinatorMarkerRel fmr = getRecombinator(call);
    List<RelNode> scans = call.getChildRels(fmr);
    for (RelNode scan : scans) {
      if (!(scan instanceof TableScan)) {
        return false;
      }
    }
    return true;
  }

  protected abstract FineoRecombinatorMarkerRel getRecombinator(RelOptRuleCall call);

  @Override
  public void onMatch(RelOptRuleCall call) {
    FineoRecombinatorMarkerRel fmr = getRecombinator(call);
    Multimap<String, RelAndRange> tables = getTables(call);
    RelOptCluster cluster = fmr.getCluster();
    RexBuilder rexer = cluster.getRexBuilder();
    Collection<RelNode> nodes = partitionReads(rexer, tables, cluster,
      fmr.getTraitSet());
    TableSetMarker marker =
      new TableSetMarker(fmr.getCluster(), fmr.getTraitSet(), fmr.getRowType());
    marker.setInputs(nodes);
    RelNode fineo = fmr.copy(fmr.getTraitSet(), of(marker));
    call.transformTo(finalTransform(fineo, call));
  }

  protected RelNode finalTransform(RelNode fineo, RelOptRuleCall call) {
    return fineo;
  }

  protected Collection<RelNode> partitionReads(RexBuilder rexer, Multimap<String, RelAndRange>
    translatedScans, RelOptCluster cluster, RelTraitSet traits) {
    Preconditions.checkState(translatedScans.size() > 0,
      "Couldn't find any tables that apply to the scan!");

    // simple case, no dynamo tables
    Collection<RelAndRange> dynamo = translatedScans.get(DYNAMO);
    Collection<RelAndRange> dfs = translatedScans.get(DFS);
    if (dynamo == null || dynamo.size() == 0) {
      return dfs.stream().map(rr -> rr.rel).collect(Collectors.toList());
    } else if (dfs == null || dfs.size() == 0) {
      // this is really weird, but I guess it could come up...
      return dynamo.stream().map(rr -> rr.rel).collect(Collectors.toList());
    }

    // TODO lookup the last 'convert' time from json -> parquet to see how far in the future we
    // can filter parquet scans and progressively 'lop' off dynamo tables and replace them with
    // parquet scans

    // right now, we just do the simple thing - find the oldest timerange for which we have a
    // table and then relegate the FS reads to everything just before that
    OptionalLong min = dynamo.stream().mapToLong(rr -> rr.start.toEpochMilli()).min();
    Preconditions.checkState(min.isPresent(),
      "Could not determine the nearline (Dynamo) table to read - no min timestamp found!");
    Instant absoluteStart = Instant.ofEpochMilli(min.getAsLong());
    RexNode startLiteral = rexer
      .makeLiteral(absoluteStart.toEpochMilli(), rexer.getTypeFactory().createSqlType(BIGINT),
        true);
    RexNode startDate = FileSystemTimestampHandler.asValueNode(absoluteStart.toEpochMilli(),
      rexer);

    List<RelNode> filteredDfs =
      dfs.stream()
         .map(rr -> rr.rel)
         .map(node -> {
           // timestamp is strictly less than the minimum dynamo time value
           RelDataTypeField field =
             node.getRowType().getField(AvroSchemaEncoder.TIMESTAMP_KEY, false, false);
           RexInputRef ref = rexer.makeInputRef(node, field.getIndex());
           RexNode limit = rexer.makeCall(LESS_THAN, ref, startLiteral);

           // don't read more directories than necessary
           RelDataTypeField dir = FileSystemTimestampHandler.getTimeDir(node);
           assert dir != null : "Didn't find a dir0 in fs scan type!";
           RexNode dirFilter = FileSystemTimestampHandler.fileScanOpToRef(rexer, node, dir,
             LESS_THAN, startDate);
           RexNode condition = RexUtil.composeConjunction(rexer, of(limit, dirFilter), true);

           // build the filter
           return new LogicalFilter(cluster, traits, node, condition);
         })
         .collect(toList());

    // add the dynamo scans
    dynamo.stream().map(rr -> rr.rel).forEach(filteredDfs::add);
    return filteredDfs;
  }

  public static class PushTimerangeFilterPastRecombinator extends
                                                          ConvertFineoMarkerIntoFilteredInputTables {

    public static final PushTimerangeFilterPastRecombinator
      INSTANCE = new PushTimerangeFilterPastRecombinator();

    private PushTimerangeFilterPastRecombinator() {
      super(operand(LogicalFilter.class, operand(FineoRecombinatorMarkerRel.class,
        unordered(operand(TableScan.class, null, Predicates.alwaysTrue(), none())))),
        "FineoPushTimerangePastRecombinatorRule");
    }

    @Override
    protected FineoRecombinatorMarkerRel getRecombinator(RelOptRuleCall call) {
      return call.rel(1);
    }

    @Override
    public Multimap<String, RelAndRange> getTables(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      FineoRecombinatorMarkerRel fmr = getRecombinator(call);
      List<RelNode> scans = call.getChildRels(fmr);
      Multimap<String, RelAndRange> translatedScans = ArrayListMultimap.create();
      String ts = FineoTable.BaseField.TIMESTAMP.getName();

      final LogicalExpression conditionExp =
        toDrill(new DrillParseContext(getPlannerSettings(call.getPlanner())), fmr, filter
          .getCondition());

      RexBuilder rexer = filter.getCluster().getRexBuilder();
      Map<String, TimestampHandler> handlers = getHandlers(rexer);
      for (RelNode s : scans) {
        TableScan scan = (TableScan) s;
        String type = getScanType(scan);
        TimestampHandler handler = handlers.get(type);
        TimestampExpressionBuilder builder =
          new TimestampExpressionBuilder(ts, handler.getBuilder(scan));
        RexNode timestamps = builder.lift(conditionExp, rexer);
        // we have to scan everything
        RelNode translated = null;
        if (!builder.isScanAll() && timestamps != null) {
          translated = handler.translateScanFromGeneratedRex(scan, timestamps);
        }
        if (translated != null) {
          translatedScans.put(type, new RelAndRange(translated, handler.getTableTimeRange(scan)));
        }
      }

      return translatedScans;
    }

    @Override
    protected RelNode finalTransform(RelNode fineo, RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      return filter.copy(filter.getTraitSet(), fineo, filter.getCondition());
    }
  }

  protected abstract Multimap<String, RelAndRange> getTables(RelOptRuleCall call);

  public static class FilterRecombinatorTablesWithNoTimestampFilter
    extends ConvertFineoMarkerIntoFilteredInputTables {
    public static final FilterRecombinatorTablesWithNoTimestampFilter INSTANCE = new
      FilterRecombinatorTablesWithNoTimestampFilter();

    private FilterRecombinatorTablesWithNoTimestampFilter() {
      super(operand(FineoRecombinatorMarkerRel.class,
        unordered(operand(TableScan.class, null, Predicates.alwaysTrue(), none()))),
        "Fineo::FilterRecombinatorTablesRule");
    }

    @Override
    protected FineoRecombinatorMarkerRel getRecombinator(RelOptRuleCall call) {
      return call.rel(0);
    }

    @Override
    public Multimap<String, RelAndRange> getTables(RelOptRuleCall call) {
      // similar to what we do above, but the table scans are completely inclusive, so we just
      // separate out the fields by type
      Multimap<String, RelAndRange> types = ArrayListMultimap.create();
      FineoRecombinatorMarkerRel fmr = getRecombinator(call);
      RexBuilder rexer = fmr.getCluster().getRexBuilder();
      Map<String, TimestampHandler> handlers = getHandlers(rexer);
      List<RelNode> scans = call.getChildRels(fmr);
      for (RelNode node : scans) {
        TableScan scan = (TableScan) node;
        String type = getScanType(scan);
        TimestampHandler handler = handlers.get(type);
        types.put(type, new RelAndRange(node, handler.getTableTimeRange(scan)));
      }

      return types;
    }
  }

  protected static String getScanType(TableScan scan) {
    List<String> name = scan.getTable().getQualifiedName();
    return name.get(0);
  }

  protected static Map<String, TimestampHandler> getHandlers(RexBuilder rexer) {
    Map<String, TimestampHandler> handlers = new HashMap<>();
    handlers.put(DFS, new FileSystemTimestampHandler(rexer));
    handlers.put(DYNAMO, new DynamoTimestampHandler(rexer));
    return handlers;
  }

  private static class RelAndRange {
    private Instant start;
    private RelNode rel;

    public RelAndRange(RelNode translated, Range<Instant> tableTimeRange) {
      this.rel = translated;
      this.start = tableTimeRange.getStart();
    }
  }
}
