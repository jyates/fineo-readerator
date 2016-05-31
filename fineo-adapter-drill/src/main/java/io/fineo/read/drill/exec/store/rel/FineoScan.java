package io.fineo.read.drill.exec.store.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Logical table scan over a Fineo table. Gets converted into a scan over multiple tables via
 * {@link FineoMultiScanRule}. This is mostly used so we can register rules
 * with the planner at the correct time.
 * <p>
 * This does not implement the actual conversion of the scan because we have to ensure that there
 * is the proper parent nodes to provide the schema lookup. Instead, this is handled in the
 * {@link FineoMultiProjectRule}.
 * </p>
 */
public class FineoScan extends TableScan implements FineoRel {

  private final List<RelOptRule> rules;
  private final Schema dynamoSchema;

  public FineoScan(RelOptCluster cluster, RelTraitSet traitSet,
    RelOptTable table, List<RelOptRule> customRules, Schema dynamoSchema) {
    super(cluster, traitSet, table);
    this.rules = customRules;
    this.dynamoSchema = dynamoSchema;
  }

  @Override
  public RelDataType deriveRowType() {
    // our row type has to match the output row type from the tables to which we are expanding
    // the scan later
    final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    Map<String, RelDataType> fields = new HashMap<>();

    for (String name : dynamoSchema.getTableNames()) {
      Table table = dynamoSchema.getTable(name);
      RelDataType row = table.getRowType(typeFactory);
      for (RelDataTypeField field : row.getFieldList()) {
        if (fields.get(field.getName()) == null) {
          fields.put(field.getName(), field.getValue());
        }
      }
    }

    RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
    for (Map.Entry<String, RelDataType> field : fields.entrySet()) {
      builder.add(field.getKey(), field.getValue());
    }

    return builder.build();
  }

  @Override
  public void register(RelOptPlanner planner) {
    for (RelOptRule rule : rules) {
      planner.addRule(rule);
    }
    super.register(planner);
  }

  public Schema getDynamoSchema() {
    return dynamoSchema;
  }
}
