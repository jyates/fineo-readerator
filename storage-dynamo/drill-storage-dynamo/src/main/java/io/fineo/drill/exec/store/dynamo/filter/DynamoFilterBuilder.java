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
import io.fineo.drill.exec.store.dynamo.spec.DynamoScanFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import java.util.List;
import java.util.function.BiFunction;

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
public class DynamoFilterBuilder
  extends AbstractExprVisitor<DynamoScanSpec, Void, RuntimeException> {

  private static final String AND = "booleanAnd";
  private static final String OR = "booleanOr";
  final private DynamoGroupScan groupScan;

  final private LogicalExpression le;
  private final DynamoTableDefinition.PrimaryKey range;
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
        this.range = pks.get(1);
      } else {
        this.range = hash;
        this.hash = pks.get(1);
      }
    } else {
      this.range = null;
    }
  }

  public DynamoScanSpec parseTree() {
    DynamoScanSpec parsedSpec = le.accept(this, null);
    if (parsedSpec != null) {
      parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getSpec(), parsedSpec);

      // figure out if this spec represents one or more Gets
      DynamoScanFilterSpec filter = parsedSpec.getFilter();
      if (filter.getHasHashKey() && filter.getHasRangeKey()) {

      }

    }
    return parsedSpec;
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public DynamoScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  @Override
  public DynamoScanSpec visitBooleanOperator(BooleanOperator op, Void value)
    throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  @Override
  public DynamoScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    DynamoScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    ImmutableList<LogicalExpression> args = call.args;

    // its a simple function call, i.e. a = '1', so just build the scan spec for that
    if (SingleFunctionProcessor.isCompareFunction(functionName)) {
      SingleFunctionProcessor processor = SingleFunctionProcessor.process(call);
      if (processor.isSuccess()) {
        nodeScanSpec = createDynamoScanSpec(call, processor);
      }
    } else {
      // its a more complicated function, of which we only handle the logical bifurcation, so
      // build up the resulting filter as tiers of conjunction/disjunction
      switch (functionName) {
        case AND:
        case OR:
          DynamoScanSpec firstScanSpec = args.get(0).accept(this, null);
          for (int i = 1; i < args.size(); ++i) {
            DynamoScanSpec nextScanSpec = args.get(i).accept(this, null);
            if (firstScanSpec != null && nextScanSpec != null) {
              nodeScanSpec = mergeScanSpecs(functionName, firstScanSpec, nextScanSpec);
            } else {
              allExpressionsConverted = false;
              if (AND.equals(functionName)) {
                nodeScanSpec = firstScanSpec == null ? nextScanSpec : firstScanSpec;
              }
            }
            firstScanSpec = nodeScanSpec;
          }
          break;
      }
    }

    // we didn't find a matching function
    if (nodeScanSpec == null) {
      allExpressionsConverted = false;
    }

    return nodeScanSpec;
  }

  private DynamoScanSpec mergeScanSpecs(String functionName, DynamoScanSpec left,
    DynamoScanSpec right) {
    DynamoScanSpec spec = new DynamoScanSpec(left);

    DynamoScanFilterSpec lf = left.getFilter();
    DynamoScanFilterSpec rf = right.getFilter();

    boolean hasHashKey;
    BiFunction<DynamoFilterSpec, DynamoFilterSpec, DynamoFilterSpec> func;
    // query/get requires that you have the hash key in the filter. AND conditions mean you can
    // have it on either side, while an OR requires it on both sides, hence the 'flipped'
    // conditional for the hash key below
    if (functionName.equals(AND)) {
      func = this::and;
      hasHashKey = lf.getHasHashKey() || rf.getHasHashKey();
    } else {
      func = this::or;
      hasHashKey = lf.getHasHashKey() && rf.getHasHashKey();
    }

    // range key just needs to be present in either side for this to have a range key, hence OR
    boolean rangeKey = lf.getHasRangeKey() || rf.getHasRangeKey();

    DynamoFilterSpec hKey = func.apply(lf.getKeyFilter(), rf.getKeyFilter());
    DynamoFilterSpec attrib = func.apply(lf.getAttributeFilter(), rf.getAttributeFilter());
    DynamoScanFilterSpec filter = new DynamoScanFilterSpec(hKey, hasHashKey, rangeKey, attrib);
    spec.setFilter(filter);
    return spec;
  }

  private DynamoFilterSpec and(DynamoFilterSpec left, DynamoFilterSpec right) {
    return left == null ? left : left.and(right);
  }

  private DynamoFilterSpec or(DynamoFilterSpec left, DynamoFilterSpec right) {
    return left == null ? left : left.or(right);
  }

  private DynamoScanSpec createDynamoScanSpec(FunctionCall call,
    SingleFunctionProcessor processor) {
    String functionName = processor.getFunctionName();
    SchemaPath field = processor.getPath();
    String fieldName = field.getAsUnescapedPath();
    Object fieldValue = processor.getValue();
    boolean isHashKey = this.hash.getName().equals(fieldName);
    boolean isRangeKey = this.range != null && this.range.getName().equals(fieldName);
    assert !(isHashKey && isRangeKey) : fieldName + " be both hash and range key";

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
    }
    DynamoFilterSpec filter = DynamoFilterSpec.create(functionName, fieldName, fieldValue);
    // we don't know how to handle this function
    if (filter == null) {
      return null;
    }

    DynamoScanSpec spec = new DynamoScanSpec(groupScan.getSpec());
    // override the filter, of which it can only be one of the three
    spec.setFilter(new DynamoScanFilterSpec(
      (isHashKey ? filter : null), isHashKey, isRangeKey,
      (!(isHashKey || isRangeKey) ? filter : null)));
    return spec;
  }
}
