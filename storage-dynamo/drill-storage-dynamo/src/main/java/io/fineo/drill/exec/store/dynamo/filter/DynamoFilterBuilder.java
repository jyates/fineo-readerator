/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fineo.drill.exec.store.dynamo.filter;

import com.google.common.collect.ImmutableList;
import io.fineo.drill.exec.store.dynamo.DynamoGroupScan;
import io.fineo.drill.exec.store.dynamo.spec.DynamoFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Filter builder heavily based on the Drill HBaseFilterBuilder. Depth-first exploration of the
 * logical expression and converts them into a filter by finding the 'leaf' expressions (i.e. a =
 * '1') and then progressively combing them with AND/OR expressions based on the function at the
 * layer above.
 * <p>
 * There is a subtle difference in how we handle nulls. If the request is <tt>a = null</tt> (with
 * an optional cast) we do an actual check for the field value being null, e.g. <tt>a = null</tt>.
 * However, if the request is <tt>isNull(a)</tt> we do <tt>attribute_exists(a)</tt>, which
 * is also a null check, but only returns if the attribute <i>has not been set</i>, compared to
 * the former, where it returns if the attribute <i><b>has been set to null</b></i>.
 * </p>
 */
public class DynamoFilterBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoFilterBuilder.class);
  private static final String AND = "booleanAnd";
  private static final String OR = "booleanOr";
  final private DynamoGroupScan groupScan;

  final private LogicalExpression le;
  private final DynamoTableDefinition.PrimaryKey rangePrimaryKey;
  private DynamoTableDefinition.PrimaryKey hash;

  private boolean allExpressionsConverted = true;

  DynamoFilterBuilder(DynamoGroupScan groupScan, LogicalExpression le) {
    this.groupScan = groupScan;
    this.le = le;

    // figure out the pks
    List<DynamoTableDefinition.PrimaryKey> pks = groupScan.getSpec().getTable().getKeys();
    this.hash = pks.get(0);
    if (pks.size() > 1) {
      if (hash.isHashKey()) {
        this.rangePrimaryKey = pks.get(1);
      } else {
        this.rangePrimaryKey = hash;
        this.hash = pks.get(1);
      }
    } else {
      this.rangePrimaryKey = null;
    }
  }

  public DynamoGroupScanSpec parseTree() {
    DynamoQuerySpecBuilder builder = new DynamoQuerySpecBuilder();
    DynamoReadBuilder parsedSpec = le.accept(builder, null);
    if (parsedSpec != null) {
      // combine with the existing scan
      return merge(this.groupScan.getSpec(), parsedSpec);
    }
    return null;
  }

  private DynamoGroupScanSpec merge(DynamoGroupScanSpec spec, DynamoReadBuilder parsedSpec) {
    // convert the spec into a read builder
    if (spec.getScan() != null) {
      DynamoReadFilterSpec scan = spec.getScan();
      parsedSpec.andScanSpec(scan);
    } else {
      List<DynamoReadFilterSpec> getOrQuery = spec.getGetOrQuery();
      if (getOrQuery.size() > 0) {
        parsedSpec.andGetOrQuery(getOrQuery);
      }
    }
    return parsedSpec.buildSpec(spec.getTable());
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  private class DynamoQuerySpecBuilder
    extends AbstractExprVisitor<DynamoReadBuilder, Void, RuntimeException> {

    @Override
    public DynamoReadBuilder visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
      allExpressionsConverted = false;
      return null;
    }

    @Override
    public DynamoReadBuilder visitBooleanOperator(BooleanOperator op, Void value)
      throws RuntimeException {
      return visitFunctionCall(op, value);
    }

    @Override
    public DynamoReadBuilder visitFunctionCall(FunctionCall call, Void value)
      throws RuntimeException {
      String functionName = call.getName();
      ImmutableList<LogicalExpression> args = call.args;

      // its a simple function call, i.e. a = '1', so just build the scan spec for that
      if (SingleFunctionProcessor.isCompareFunction(functionName)) {
        DynamoReadBuilder builder = null;
        SingleFunctionProcessor processor = SingleFunctionProcessor.process(call);
        if (processor.isSuccess()) {
          FilterFragment fragment = createDynamoFilter(processor);
          if (fragment == null) {
            allExpressionsConverted = false;
          }
          builder = new DynamoReadBuilder(rangePrimaryKey == null);
          builder.and(fragment);
        }
        return builder;
      }

      // its a more complicated function, so try and break it down as a combination of and/or
      switch (functionName) {
        case AND:
        case OR:
          DynamoReadBuilder builder = new DynamoReadBuilder(rangePrimaryKey == null);
          for (LogicalExpression expr : args) {
            DynamoReadBuilder exprBuilder = expr.accept(this, null);
            if (exprBuilder == null) {
              allExpressionsConverted = false;
              continue;
            }
            build:
            switch (functionName) {
              case AND:
                builder.and(exprBuilder);
                break build;
              case OR:
                builder.or(exprBuilder);
                break build;
            }
          }
          return builder;
      }

      return null;
    }

    private FilterFragment createDynamoFilter(SingleFunctionProcessor processor) {
      String functionName = processor.getFunctionName();
      SchemaPath field = processor.getPath();
      String fieldName = field.getAsUnescapedPath();
      Object fieldValue = processor.getValue();
      boolean isHashKey = DynamoFilterBuilder.this.hash.getName().equals(fieldName);
      boolean isRangeKey = DynamoFilterBuilder.this.rangePrimaryKey != null &&
                           DynamoFilterBuilder.this.rangePrimaryKey.getName().equals(fieldName);
      assert !(isHashKey && isRangeKey) : fieldName + " is both hash and rangePrimaryKey key";
      boolean equals = false;
      boolean equalityTest = false;

      // normalize function names, since drill can't do this already...apparently
      switch (functionName) {
        case "isnull":
        case "isNull":
        case "is null":
          functionName = "isNull";
          break;
        case "isnotnull":
        case "isNotNull":
        case "is not null":
          functionName = "isNotNull";
          break;
        case "equal":
          equals = true;
        case "not_equal":
        case "greater_than_or_equal_to":
        case "greater_than":
        case "less_than_or_equal_to":
        case "less_than":
          equalityTest = true;
          break;
      }
      DynamoFilterSpec filter = DynamoFilterSpec.create(functionName, fieldName, fieldValue);
      // we don't know how to handle this function
      if (filter == null) {
        return null;
      }

      return new FilterFragment(filter, equals, equalityTest, isHashKey, isRangeKey);
    }
  }
}
