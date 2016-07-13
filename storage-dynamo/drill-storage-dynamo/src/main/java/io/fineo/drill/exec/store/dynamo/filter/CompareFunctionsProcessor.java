/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fineo.drill.exec.store.dynamo.filter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

class CompareFunctionsProcessor extends
                                AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {
  private String value;
  private boolean success;
  private boolean isEqualityFn;
  private SchemaPath path;
  private String functionName;

  public static boolean isCompareFunction(String functionName) {
    return COMPARE_FUNCTIONS_TRANSPOSE_MAP.keySet().contains(functionName);
  }

  public static CompareFunctionsProcessor process(FunctionCall call){
    String functionName = call.getName();
    LogicalExpression nameArg = call.args.get(0);
    LogicalExpression valueArg = call.args.size() >= 2 ? call.args.get(1) : null;
    CompareFunctionsProcessor evaluator = new CompareFunctionsProcessor(functionName);

    if (valueArg != null) { // binary function
      if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
        LogicalExpression swapArg = valueArg;
        valueArg = nameArg;
        nameArg = swapArg;
        evaluator.functionName = COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName);
      }
      evaluator.success = nameArg.accept(evaluator, valueArg);
    } else if (call.args.get(0) instanceof SchemaPath) {
      evaluator.success = true;
      evaluator.path = (SchemaPath) nameArg;
    }

    return evaluator;
  }

  public CompareFunctionsProcessor(String functionName) {
    this.success = false;
    this.functionName = functionName;
    this.isEqualityFn = COMPARE_FUNCTIONS_TRANSPOSE_MAP.containsKey(functionName)
        && COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName).equals(functionName);
  }

  public String getValue() {
    return value;
  }

  public boolean isSuccess() {
    return success;
  }

  public SchemaPath getPath() {
    return path;
  }

  public String getFunctionName() {
    return functionName;
  }

  @Override
  public Boolean visitCastExpression(CastExpression e, LogicalExpression valueArg) throws RuntimeException {
    if (e.getInput() instanceof CastExpression || e.getInput() instanceof SchemaPath) {
      return e.getInput().accept(this, valueArg);
    }
    return false;
  }

@Override
  public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg) throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg) throws RuntimeException {
    if (valueArg instanceof QuotedString) {
      this.value = ((QuotedString) valueArg).value;
      this.path = path;
      return true;
    }
    return false;
  }

  private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES;
  static {
    ImmutableSet.Builder<Class<? extends LogicalExpression>> builder = ImmutableSet.builder();
    VALUE_EXPRESSION_CLASSES = builder
        .add(BooleanExpression.class)
        .add(DateExpression.class)
        .add(DoubleExpression.class)
        .add(FloatExpression.class)
        .add(IntExpression.class)
        .add(LongExpression.class)
        .add(QuotedString.class)
        .add(TimeExpression.class)
        // TODO add support for IN and BETWEEN
        .build();
  }

  private static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;
  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
        // unary functions
        .put("isnotnull", "isnotnull")
        .put("isNotNull", "isNotNull")
        .put("is not null", "is not null")
        .put("isnull", "isnull")
        .put("isNull", "isNull")
        .put("is null", "is null")
        // binary functions
        .put("like", "like")
        .put("equal", "equal")
        .put("not_equal", "not_equal")
        .put("greater_than_or_equal_to", "less_than_or_equal_to")
        .put("greater_than", "less_than")
        .put("less_than_or_equal_to", "greater_than_or_equal_to")
        .put("less_than", "greater_than")
        .build();
  }

}
