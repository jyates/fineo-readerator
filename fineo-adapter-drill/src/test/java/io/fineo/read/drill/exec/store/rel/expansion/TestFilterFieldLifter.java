package io.fineo.read.drill.exec.store.rel.expansion;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.drill.exec.planner.types.DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestFilterFieldLifter {
  private static final RelDataTypeFactory factory =
    new SqlTypeFactoryImpl(DRILL_REL_DATATYPE_SYSTEM);
  private static final RexBuilder builder = new RexBuilder(factory);

  @Test
  public void testSimpleEquals() throws Exception {
    RelDataType type = dynamicType();
    String field = "f1";
    RexInputRef ref = getFieldRef(type, field);

    RexNode equals = builder.makeCall(SqlStdOperatorTable.EQUALS, ref, builder.makeLiteral(true));
    FilterFieldLifter lifter = new FilterFieldLifter(equals, type, builder, field);
    assertEquals(equals.toString(), lifter.lift().toString());
  }

  @Test
  public void testSimpleConjunctionOnField() throws Exception {
    RelDataType type = dynamicType();
    String field = "f1";
    RexInputRef ref = getFieldRef(type, field);

    RexNode and = RexUtil.composeConjunction(builder, of(equalsTrue(ref), equalsTrue(ref)), true);
    FilterFieldLifter lifter = new FilterFieldLifter(and, type, builder, field);
    assertEquals(and.toString(), lifter.lift().toString());
  }

  @Test
  public void testConjunctionButOnlyReturnRequiredField() throws Exception {
    RelDataType type = dynamicType();
    String field = "f1";
    RexInputRef ref = getFieldRef(type, field);
    RexInputRef ref2 = getFieldRef(type, "f2");

    RexNode equals = equalsTrue(ref);
    RexNode and = RexUtil.composeConjunction(builder, of(equals, equalsTrue(ref2)), true);
    FilterFieldLifter lifter = new FilterFieldLifter(and, type, builder, field);
    assertEquals(equals.toString(), lifter.lift().toString());
  }

  @Test
  public void testDisjunctionButOnlyReturnRequiredField() throws Exception {
    RelDataType type = dynamicType();
    String field = "f1";
    RexInputRef ref = getFieldRef(type, field);
    RexInputRef ref2 = getFieldRef(type, "f2");

    RexNode equals = equalsTrue(ref);
    RexNode or = RexUtil.composeDisjunction(builder, of(equals, equalsTrue(ref2)), true);
    FilterFieldLifter lifter = new FilterFieldLifter(or, type, builder, field);
    assertNull(lifter.lift());
  }

  @Test
  public void testDisjunctionOnTwoRequiredFields() throws Exception {
    RelDataType type = dynamicType();
    String field = "f1", field2 = "f2";
    RexInputRef ref = getFieldRef(type, field);
    RexInputRef ref2 = getFieldRef(type, field2);

    RexNode or = RexUtil.composeDisjunction(builder, of(equalsTrue(ref), equalsTrue(ref2)), true);
    FilterFieldLifter lifter = new FilterFieldLifter(or, type, builder, field, field2);
    assertEquals(or.toString(), lifter.lift().toString());
  }

  private RexNode equalsTrue(RexNode node) {
    return builder.makeCall(SqlStdOperatorTable.EQUALS, node, builder.makeLiteral(true));
  }

  private RelDataType dynamicType() {
    RelDataTypeHolder holder = new RelDataTypeHolder();
    return new RelDataTypeDrillImpl(holder, factory);
  }

  private RexInputRef getFieldRef(RelDataType type, String field) {
    RelDataTypeField ft = type.getField(field, true, true);
    return new RexInputRef(ft.getIndex(), type);
  }
}
