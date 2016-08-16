package io.fineo.read.drill.exec.store.rel.expansion.rule;

import io.fineo.read.drill.exec.store.rel.expansion.rule.LiftingRexVisitor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.drill.exec.planner.types.DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM;
import static org.junit.Assert.assertEquals;

public class TestLiftingRexVisitor {

  @Test
  public void testNoConjunctionLift() throws Exception {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(DRILL_REL_DATATYPE_SYSTEM);
    RexBuilder builder = new RexBuilder(factory);
    RelDataTypeHolder holder = new RelDataTypeHolder();
    RelDataType type = new RelDataTypeDrillImpl(holder, factory);
    String field = "f1";
    RelDataTypeField ft = type.getField(field, true, true);
    RexInputRef ref = new RexInputRef(ft.getIndex(), type);

    RexNode equals = builder.makeCall(SqlStdOperatorTable.EQUALS, ref, builder.makeLiteral(true));
    LiftingRexVisitor visitor = new LiftingRexVisitor(builder, of(ft.getIndex()));
    assertEquals(equals.toString(), equals.accept(visitor).toString());
  }


}
