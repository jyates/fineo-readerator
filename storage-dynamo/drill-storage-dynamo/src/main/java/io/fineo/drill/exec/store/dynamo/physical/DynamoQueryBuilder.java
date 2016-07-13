package io.fineo.drill.exec.store.dynamo.physical;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import io.fineo.drill.exec.store.dynamo.spec.DynamoFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoFilterSpec.FilterLeaf;
import io.fineo.drill.exec.store.dynamo.spec.DynamoFilterSpec.FilterNodeInner;
import io.fineo.drill.exec.store.dynamo.spec.DynamoScanFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newLinkedList;
import static io.fineo.drill.exec.store.dynamo.physical.DynamoRecordReader.COMMAS;

/**
 *
 */
public class DynamoQueryBuilder {

  private DynamoSubScan.DynamoSubScanSpec slice;
  private DynamoScanSpec scanSpec;
  private ParallelScanProperties scanProps;
  private boolean consistentRead;
  private boolean isStarQuery = true;
  private List<String> columns;

  public DynamoQueryBuilder withSlice(DynamoSubScan.DynamoSubScanSpec slice) {
    this.slice = slice;
    return this;
  }

  public DynamoQueryBuilder withScanSpec(DynamoScanSpec scanSpec) {
    this.scanSpec = scanSpec;
    return this;
  }

  public DynamoQueryBuilder withProps(ParallelScanProperties scanProps) {
    this.scanProps = scanProps;
    return this;
  }

  public DynamoQueryBuilder withConsistentRead(boolean consistentRead) {
    this.consistentRead = consistentRead;
    return this;
  }

  public DynamoQueryBuilder withColumns(List<String> columns) {
    this.columns = columns;
    this.isStarQuery = false;
    return this;
  }

  public Iterator<Page<Item, ?>> query(AmazonDynamoDBAsyncClient client) {
    DynamoScanFilterSpec filters = scanSpec.getFilter();
    DynamoTableDefinition tableDef = scanSpec.getTable();
    Table table = new DynamoDB(client).getTable(tableDef.getName());
    if (filters.getKeyFilter() != null) {
      Iterator iter;
      if (tableDef.getKeys().size() == 1 ) {
        iter = buildGet(table);
      } else {
        iter = buildQuery(table);
      }
      return iter;
    }
    Iterator iter = buildScan(table); ;
    return iter;
  }

  private Iterator<Page<Item, ScanOutcome>> buildScan(Table table) {
    ScanSpec scan = new ScanSpec();
    scan.withConsistentRead(consistentRead);
    // basic scan requirements
    int limit = scanProps.getLimit();
    if (limit > 0) {
      scan.setMaxPageSize(limit);
    }
    scan.withSegment(slice.getSegmentId()).withTotalSegments(slice.getTotalSegments());
    if (!isStarQuery) {
      scan.withProjectionExpression(COMMAS.join(columns));
    }
    ItemCollection<ScanOutcome> results = table.scan(scan);
    return results.pages().iterator();
  }

  private Iterator<Page<Item, QueryOutcome>> buildQuery(Table table) {
    QuerySpec query = new QuerySpec();
    query.withConsistentRead(consistentRead);
    query.withMaxPageSize(scanProps.getLimit());
    if (!isStarQuery) {
      query.withProjectionExpression(COMMAS.join(columns));
    }
    NameMapper mapper = new NameMapper();

    DynamoScanFilterSpec filter = scanSpec.getFilter();
    DynamoFilterSpec attribute = filter.getAttributeFilter();
    String attrFilterExpr = asFilterExpression(mapper, attribute);
    if (attrFilterExpr != null) {
      query.withFilterExpression(attrFilterExpr);
    }

    query.withNameMap(mapper.nameMap);
    query.withValueMap(mapper.valueMap);

    return table.query(query).pages().iterator();
  }

  private Iterator<Page<Item, ?>> buildGet(Table table) {
    GetItemSpec spec = new GetItemSpec();
    return null;
  }

  private String asFilterExpression(NameMapper mapper, DynamoFilterSpec spec) {
    if (spec == null) {
      return null;
    }
    DynamoFilterSpec.FilterTree tree = spec.getTree();
    // replace the leaf values in the tree with expressions
    List<DynamoFilterSpec.FilterNode> nodes = newLinkedList();
    nodes.add(tree.getRoot());
    DynamoFilterSpec.FilterNode node;
    while (nodes.size() > 0) {
      node = nodes.remove(0);
      if (node instanceof FilterLeaf) {
        FilterLeaf leaf = (FilterLeaf) node;
        leaf = new FilterLeaf(mapper.name(leaf.getKey()), leaf.getOperand(),
          mapper.value(leaf.getValue()));
        // must be a depth 1 tree
        if (node.getParent() == null) {
          assert node == tree.getRoot() : "No has no parents, but is not the root node!";
          tree = new DynamoFilterSpec.FilterTree(leaf);
          break;
        }
        ((FilterNodeInner) node.getParent()).update(node, leaf);
      } else {
        FilterNodeInner inner = (FilterNodeInner) node.getParent();
        nodes.add(inner.getLeft());
        nodes.add(inner.getRight());
      }
    }
    return tree.toString();
  }

  private class NameMapper {
    private int counter = 0;
    Map<String, String> nameMap = new HashMap<>();
    Map<String, Object> valueMap = new HashMap<>();

    public String name(String name) {
      return add(":n", name, nameMap);
    }

    public String value(Object value) {
      return add("#s", value, valueMap);
    }

    private <IN extends Object> String add(String prefix, IN in, Map<String, IN> map) {
      String out = prefix + (counter++);
      map.put(out, in);
      return out;
    }

  }
}
