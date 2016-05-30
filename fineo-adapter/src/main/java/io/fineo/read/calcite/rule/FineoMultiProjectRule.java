package io.fineo.read.calcite.rule;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.internal.customer.Metric;
import io.fineo.read.calcite.rel.FineoRecombinatorRel;
import io.fineo.read.calcite.rel.FineoScan;
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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;

/**
 * Converts a projection + filter into a projection + filter across all the possible field names
 * in the underlying table and combined back into a single relation via the
 * {@link FineoRecombinatorRel}
 */
public class FineoMultiProjectRule extends RelOptRule {

  private static final List<String> REQUIRED_FIELDS =
    newArrayList(ORG_ID_KEY, ORG_METRIC_TYPE_KEY).stream().map(String::toUpperCase).collect(
      Collectors.toList());

  private final SchemaStore store;

  public FineoMultiProjectRule(SchemaStore store) {
    // match a project that has a filter
    super(operand(LogicalProject.class,
      operand(LogicalFilter.class,
        operand(FineoScan.class, RelOptRule.any()))));
    this.store = store;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(1);

    // make sure that this filter includes the metric info we need to lookup the expanded fields
    RelDataType rowType = filter.getRowType();
    return new CompanyAndMetricFiltered(rowType.getFieldNames()).test(filter);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalProject project = call.rel(0);
    LogicalFilter filter = call.rel(1);
    RelDataType rowType = project.getRowType();

    // lookup the metric/field alias information
    Map<String, String> metricLookup = new HashMap<>(2);
    lookupMetricFieldsFromFilter(filter, metricLookup, rowType.getFieldNames());
    // need the uppercase names here because that's how we pulled them out of the query
    AvroSchemaManager schema =
      new AvroSchemaManager(store, metricLookup.get(ORG_ID_KEY.toUpperCase()));
    Metric metric = schema.getMetricInfo(metricLookup.get(ORG_METRIC_TYPE_KEY.toUpperCase()));
    Map<String, List<String>> cnamesToAlias = metric.getMetadata().getCanonicalNamesToAliases();
    Map<String, String> aliasToCanonicalName = AvroSchemaManager.getAliasRemap(metric);

    // expand the fields in the metric to the new projected fields, matching the field types to
    // the original types
    Multimap<String, String> expanded = ArrayListMultimap.create();
    project.getNamedProjects().stream()
           .map(pair -> pair.getValue())
           .filter(IS_BASE_FIELD_IN_QUERY.negate())
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
    project = LogicalProject.create(filter, projectedFields, rowType);
    call.transformTo(new FineoRecombinatorRel(project, rowType, store));
  }

  private void lookupMetricFieldsFromFilter(LogicalFilter filter,
    Map<String, String> metricLookup, List<String> fieldNames) {
    FilterFieldHandler handler = new FilterFieldHandler(fieldNames);
    handler.handle(filter, (parser, name, value) -> {
      // its a field we expect, get the metric
      if (REQUIRED_FIELDS.contains(parser.getFieldName())) {
        metricLookup.put(parser.getFieldName(), getFieldValue(value));
      }
    });
  }

  private String getFieldValue(RexNode node) {
    RexLiteral lit = (RexLiteral) node;
    return ((NlsString) lit.getValue()).getValue();
  }

  private static class CompanyAndMetricFiltered implements Predicate<LogicalFilter> {
    private final FilterFieldHandler handler;

    private CompanyAndMetricFiltered(List<String> fieldNames) {
      this.handler = new FilterFieldHandler(fieldNames);
    }

    @Override
    public boolean test(@Nullable LogicalFilter filter) {
      List<String> expected = newArrayList(REQUIRED_FIELDS);
      handler.handle(filter, (parser, n, v) -> {
        // its a field name
        String name = parser.getFieldName();
        expected.remove(name);
      });

      // all the expected field were part of this filter
      return expected.size() == 0;
    }
  }

  private static class FilterFieldHandler {

    private final List<String> fieldNames;

    protected FilterFieldHandler(List<String> fieldNames) {
      this.fieldNames = fieldNames;
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
        FieldNameParser parser = new FieldNameParser(fieldNames, name);

        // expression is actually value = name, so swap arguments
        if (!parser.isField()) {
          parser = new FieldNameParser(fieldNames, value);
          RexNode tmp = value;
          value = name;
          name = tmp;
          assert parser.isField();
        }
        // its a field name
        callback.handle(parser, name, value);
      }
    }
  }

  @FunctionalInterface
  private interface FieldCallback {
    void handle(FieldNameParser parser, RexNode name, RexNode value);
  }

  private static class FieldNameParser {
    private String fieldName;

    private FieldNameParser(List<String> fieldNames, RexNode node) {
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

  private static final Predicate<String> IS_BASE_FIELD_IN_QUERY =
    field -> AvroSchemaEncoder.IS_BASE_FIELD.test(field.toLowerCase());
}
