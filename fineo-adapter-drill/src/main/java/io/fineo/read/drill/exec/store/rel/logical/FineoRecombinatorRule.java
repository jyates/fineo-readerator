package io.fineo.read.drill.exec.store.rel.logical;

import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.exec.store.rel.FineoRecombinatorMarkerRel;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.NlsString;
import org.apache.drill.exec.planner.logical.DrillRel;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;

/**
 * Converts a projection + filter into a projection + filter across all the possible field names
 * in the underlying table and combined back into a single relation via the
 * {@link FineoRecombinatorMarkerRel}
 */
public class FineoRecombinatorRule extends RelOptRule {

  private static final List<String> REQUIRED_FIELDS = newArrayList(ORG_ID_KEY, ORG_METRIC_TYPE_KEY);

  private static final Predicate<LogicalFilter> PREDICATE = new CompanyAndMetricFiltered();

  public FineoRecombinatorRule() {
    // match a project that has a filter
    super(operand(LogicalProject.class,
      operand(LogicalFilter.class,
        operand(FineoRecombinatorMarkerRel.class, RelOptRule.any()))), "FineoRecombinatorRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(1);

    // make sure that this filter includes the type/metric info to lookup the expanded fields
    return PREDICATE.test(filter);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalProject project = call.rel(0);
    LogicalFilter filter = call.rel(1);
    FineoRecombinatorMarkerRel frr = call.rel(2);

    // lookup the metric/field alias information
    Map<String, String> metricLookup = lookupMetricFieldsFromFilter(filter);
    // need the uppercase names here because that's how we pulled them out of the query
    SchemaStore store = frr.getStore();
    AvroSchemaManager schema = new AvroSchemaManager(store, metricLookup.get(ORG_ID_KEY));
    Metric metric = schema.getMetricInfo(metricLookup.get(ORG_METRIC_TYPE_KEY));

    // This is actually a marker for a set of logical unions between types
    RelBuilder builder = RelBuilder.proto(call.getPlanner().getContext())
                                   .create(project.getCluster(), frr.getRelSchema());

    RelDataType rowType = frr.getRowType();
    // each input is wrapped with an FRR to normalize output types
    int scanCount = 0;
    for (RelNode relNode : frr.getInputs()) {
      RelNode convertedInput = convert(relNode, relNode.getTraitSet().plus(DrillRel.DRILL_LOGICAL));
      FineoRecombinatorRel rel =
        new FineoRecombinatorRel(frr.getCluster(), convertedInput.getTraitSet(), convertedInput,
          metric, rowType);
      builder.push(rel);
      scanCount++;
    }

    // combine the subqueries with a set of unions
    for (int i = 0; i < scanCount - 1; i++) {
      builder.union(true);
    }
    // result needs to be sorted on the timestamp
    addSort(builder, project.getCluster());

    RelNode rel = builder.build();

    // rebuild the tree above us. We cannot use the existing stack b/c the subsets are messed up.
    // However, when we point to frr's input, a LOGICAL RelSubset, we are pointing to an equivalence
    // of the FRMR's subset.
    filter = LogicalFilter.create(rel, filter.getCondition());
    project = LogicalProject.create(filter, project.getProjects(), project.getRowType());
    call.transformTo(project);
  }

  private void addSort(RelBuilder builder, RelOptCluster cluster) {
    RelNode node = builder.peek();
    RelDataType type = node.getRowType();
    RelDataTypeField field = type.getField(AvroSchemaEncoder.TIMESTAMP_KEY, false, false);
    RexNode sortNode = cluster.getRexBuilder().makeInputRef(node, field.getIndex());
    builder.sort(sortNode);
  }

  private Map<String, String> lookupMetricFieldsFromFilter(LogicalFilter filter) {
    Map<String, String> metricLookup = new HashMap<>();
    FilterFieldHandler handler = new FilterFieldHandler(filter);
    handler.handle(filter, (parser, name, value) -> {
      // its a field we expect, get the metric
      if (REQUIRED_FIELDS.contains(parser.getFieldName())) {
        metricLookup.put(parser.getFieldName().toLowerCase(), getFieldValue(value));
      }
    });
    return metricLookup;
  }

  private String getFieldValue(RexNode node) {
    RexLiteral lit = (RexLiteral) node;
    return ((NlsString) lit.getValue()).getValue();
  }

  private static class CompanyAndMetricFiltered implements Predicate<LogicalFilter> {

    @Override
    public boolean test(@Nullable LogicalFilter filter) {
      FilterFieldHandler handler = new FilterFieldHandler(filter);
      List<String> expected = newArrayList(REQUIRED_FIELDS);
      handler.handle(filter, (parser, n, v) -> {
        // its a field name
        String name = parser.getFieldName();
        expected.remove(name.toLowerCase());
      });

      // all the expected field were part of this filter
      return expected.size() == 0;
    }
  }

  private static class FilterFieldHandler {

    private final List<String> fieldNames;

    protected FilterFieldHandler(LogicalFilter filter) {
      this.fieldNames = filter.getRowType().getFieldNames();
    }

    public void handle(LogicalFilter filter, FieldCallback callback) {
      RexCall condition = (RexCall) filter.getCondition();
      for (RexNode node : condition.getOperands()) {
        // i.e. =, <, >, etc.
        RexCall call = (RexCall) node;
        List<RexNode> leftRight = call.getOperands();
        assert leftRight.size() == 2;
        RexNode name = leftRight.get(0);
        RexNode value = leftRight.get(1);

        SimpleFieldNameParser parser = new SimpleFieldNameParser(fieldNames, name);

        // expression is actually value = name, so swap arguments
        if (!parser.isSimpleField()) {
          parser = new SimpleFieldNameParser(fieldNames, value);
          // its not a simple field, skip it, we only care about finding the required fields
          if (!parser.isSimpleField()) {
            continue;
          }
          RexNode tmp = value;
          value = name;
          name = tmp;
        }

        assert parser.isSimpleField();
        // its a field name
        callback.handle(parser, name, value);
      }
    }
  }

  @FunctionalInterface
  private interface FieldCallback {
    void handle(SimpleFieldNameParser parser, RexNode name, RexNode value);
  }

  /**
   * Parse out the field name from the {@link RexNode}, if the node is a simple {@link RexInputRef}
   */
  private static class SimpleFieldNameParser {
    private String fieldName;

    private SimpleFieldNameParser(List<String> fieldNames, RexNode node) {
      if (node instanceof RexInputRef) {
        RexInputRef ref = (RexInputRef) node;
        this.fieldName = fieldNames.get(ref.getIndex());
      }
    }

    public boolean isSimpleField() {
      return this.fieldName != null;
    }

    public String getFieldName() {
      return this.fieldName;
    }
  }

  private static final Predicate<String> IS_BASE_FIELD_IN_QUERY =
    field -> AvroSchemaEncoder.IS_BASE_FIELD.test(field.toLowerCase());
}
