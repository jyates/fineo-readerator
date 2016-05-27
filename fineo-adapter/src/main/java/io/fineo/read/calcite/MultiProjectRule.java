package io.fineo.read.calcite;

import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;

/**
 * Converts a projection + filter into a projection +filter across all the possible field names
 * in the underlying table, rather than the queried fields
 */
public class MultiProjectRule extends RelOptRule {

  private static final List<String> REQUIRED_FIELDS = newArrayList(ORG_ID_KEY, ORG_METRIC_TYPE_KEY);

  private final SchemaStore store;

  public MultiProjectRule(SchemaStore store) {
    // match a project that has a filter
    super(operand(LogicalProject.class, operand(LogicalFilter.class, RelOptRule.any())));
    this.store = store;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalProject project = call.rel(0);
    LogicalFilter filter = call.rel(1);

    // make sure that this filter includes the metric info we need to lookup the expanded fields
    RelDataType rowType = project.getRowType();
    return new CompanyAndMetricFiltered(rowType.getFieldNames()).apply(filter);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalProject project = call.rel(0);
    LogicalFilter filter = call.rel(1);
    RelDataType rowType = project.getRowType();

    // lookup the metric/field alias information
    Map<String, String> metricLookup = new HashMap<>(2);
    lookupMetricFieldsFromFilter(filter, metricLookup, rowType.getFieldNames());
    AvroSchemaManager schema = new AvroSchemaManager(store, metricLookup.get(ORG_ID_KEY));
    Metric metric = schema.getMetricInfo(metricLookup.get(ORG_METRIC_TYPE_KEY));
    Map<String, List<String>> cnamesToAlias = metric.getMetadata().getCanonicalNamesToAliases();
    Map<String, String> aliasToCanonicalName = AvroSchemaManager.getAliasRemap(metric);

    // expand the fields in the metric to the new projected fields, matching the field types to
    // the original types
    Multimap<String, String> expanded = ArrayListMultimap.create();
    project.getNamedProjects().stream()
           .map(pair -> pair.getValue())
           .filter(AvroSchemaEncoder.IS_BASE_FIELD.negate())
           .forEach(aliasName -> {
             String cname = aliasToCanonicalName.get(aliasName);
             List<String> aliases = cnamesToAlias.get(cname);
             expanded.putAll(aliasName, aliases);
           });

    // build a new list of projections and row data types
    final List<Pair<String, RelDataType>> aliasComponents = new ArrayList<>();

    final List<RelDataTypeField> fields = rowType.getFieldList();
    final RelDataTypeFactory.FieldInfoBuilder builder =
      project.getCluster().getTypeFactory().builder();
    int index = 0;
    for (RelDataTypeField field : fields) {
      RelDataType type = field.getType();
      assert null != type;
      // find all the aliases that match this name
      String name = field.getName();
      builder.add(field.getName(), type);
      Collection<String> aliases = expanded.get(name);
      if (aliases != null) {
        for (String alias : aliases) {
          aliasComponents.add(new Pair<>(alias, type));
        }
      }
      index++;
    }

    // add the alias fields to the end of the row type. Also, create a new field projection based
    // on the new, alias field indexes
    List<RexNode> projectedFields = newArrayList(project.getProjects());
    for (Pair<String, RelDataType> alias : aliasComponents) {
      builder.add(alias.getKey(), alias.getValue());
      projectedFields.add(new RexInputRef(index, alias.getValue()));
      index++;
    }

    rowType = builder.build();
    call.transformTo(LogicalProject.create(filter, projectedFields, rowType));
  }

  private void lookupMetricFieldsFromFilter(LogicalFilter filter,
    Map<String, String> metricLookup, List<String> fieldNames) {
    RexCall condition = (RexCall) filter.getCondition();
    for (RexNode node : condition.getOperands()) {
      // i.e. =, <, >, etc.
      RexCall called = (RexCall) node;
      List<RexNode> leftRight = ((RexCall) called).getOperands();
      assert leftRight.size() == 2;
      RexNode name = leftRight.get(0);
      RexNode value = leftRight.get(0);
      FieldNameParser parser = new FieldNameParser(fieldNames, name);
      // not a field name, switch to the right
      if (!parser.isField()) {
        name = value;
        parser = new FieldNameParser(fieldNames, name);
        value = name;
      }

      // its a field we expect, get the metric
      if (REQUIRED_FIELDS.contains(parser.getFieldName())) {
        metricLookup.put(parser.getFieldName(), getFieldValue(value));
      }
    }
  }

  private String getFieldValue(RexNode node) {
    RexLiteral lit = (RexLiteral) node;
    return ((NlsString) lit.getValue()).getValue();
  }

  private static class CompanyAndMetricFiltered implements Predicate<LogicalFilter> {
    private final List<String> fieldNames;

    private CompanyAndMetricFiltered(List<String> fieldNames) {
      this.fieldNames = fieldNames;
    }

    @Override
    public boolean apply(@Nullable LogicalFilter filter) {
      RexCall condition = (RexCall) filter.getCondition();
      List<String> expected = newArrayList(REQUIRED_FIELDS);
      for (RexNode node : condition.getOperands()) {
        // i.e. =, <, >, etc.
        RexCall call = (RexCall) node;
        List<RexNode> leftRight = ((RexCall) call).getOperands();
        assert leftRight.size() == 2;
        RexNode left = leftRight.get(0);
        FieldNameParser parser = new FieldNameParser(fieldNames, left);

        if (!parser.isField()) {
          parser = new FieldNameParser(fieldNames, leftRight.get(1));
          assert parser.isField();
        }
        // its a field name
        String name = parser.getFieldName();
        expected.remove(name);
      }
      // all the expected field were part of this filter
      return expected.size() == 0;
    }
  }

  private static class FieldNameParser {
    private String fieldName;
    private final RexNode node;

    private FieldNameParser(List<String> fieldNames, RexNode node) {
      this.node = node;

      if (node instanceof RexCall) {
        RexCall call = (RexCall) node;
        assert call.getKind() == SqlKind.CAST;
        RexInputRef ref = (RexInputRef) call.getOperands().get(0);
        this.fieldName = fieldNames.get(ref.getIndex());
      }
    }

    public boolean isField() {
      return this.fieldName != null;
    }

    public String getFieldName() {
      return this.fieldName;
    }
  }
}
