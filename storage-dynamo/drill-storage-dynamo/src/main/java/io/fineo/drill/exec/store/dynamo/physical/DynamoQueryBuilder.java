package io.fineo.drill.exec.store.dynamo.physical;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.google.common.collect.AbstractIterator;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import io.fineo.drill.exec.store.dynamo.spec.DynamoFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoFilterSpec.FilterLeaf;
import io.fineo.drill.exec.store.dynamo.spec.DynamoFilterSpec.FilterNodeInner;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubReadSpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubScanSpec;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newLinkedList;
import static io.fineo.drill.exec.store.dynamo.physical.DynamoScanRecordReader.COMMAS;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

/**
 * Handle the conversion from specs to a dynamo query
 */
public class DynamoQueryBuilder {

  private DynamoSubReadSpec slice;
  private ParallelScanProperties scanProps;
  private boolean consistentRead;
  private boolean isStarQuery = true;
  private List<String> columns;
  private DynamoTableDefinition tableDef;

  public DynamoQueryBuilder withSlice(DynamoSubReadSpec slice) {
    this.slice = slice;
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

  public DynamoQuery build(AmazonDynamoDBAsyncClient client) {
    return new DynamoQuery(new DynamoDB(client).getTable(tableDef.getName()),
      columns == null || columns.size() == 0 ? "" : COMMAS.join(columns));
  }

  public class DynamoQuery {
    private final Table table;
    private final String projection;

    public DynamoQuery(Table table, String projection) {
      this.table = table;
      this.projection = projection;
    }

    public Iterator<Page<Item, ?>> scan() {
      ScanSpec scan = new ScanSpec();
      scan.withConsistentRead(consistentRead);
      // basic scan requirements
      int limit = scanProps.getLimit();
      if (limit > 0) {
        scan.setMaxPageSize(limit);
      }
      DynamoSubScanSpec scanSpec = (DynamoSubScanSpec) DynamoQueryBuilder.this.slice;
      scan.withSegment(scanSpec.getSegmentId()).withTotalSegments(scanSpec.getTotalSegments());
      if (!isStarQuery) {
        scan.withProjectionExpression(projection);
      }

      // scans only have a single filter since they read everything. Thus we can combine the key
      // and attributes with AND and make a single filter.
      NameMapper mapper = new NameMapper();
      DynamoReadFilterSpec filterSpec = slice.getFilter();
      DynamoFilterSpec key = filterSpec.getKeyFilter();
      DynamoFilterSpec attribute = filterSpec.getAttributeFilter();
      DynamoFilterSpec filter = key == null ? attribute : key.and(attribute);
      String filterString = asFilterExpression(mapper, filter);
      if (filterString != null) {
        scan.withFilterExpression(filterString);
        scan.withNameMap(mapper.nameMap);
        scan.withValueMap(mapper.valueMap);
      }
      ItemCollection<ScanOutcome> results = table.scan(scan);
      Iterator iter = results.pages().iterator();
      return iter;
    }

    public Iterator<Page<Item, ?>> query() {
      QuerySpec query = new QuerySpec();
      query.withConsistentRead(consistentRead);
      query.withMaxPageSize(scanProps.getLimit());
      if (!isStarQuery) {
        query.withProjectionExpression(projection);
      }
      NameMapper mapper = new NameMapper();
      DynamoReadFilterSpec filter = slice.getFilter();

      // key space
      DynamoFilterSpec key = filter.getKeyFilter();
      String keyFilter = asFilterExpression(mapper, key);
      assert keyFilter != null : "Got a null key filter for query! Spec: " + slice;
      query.withKeyConditionExpression(keyFilter);

      // attributes, if we have them
      DynamoFilterSpec attribute = filter.getAttributeFilter();
      String attrFilterExpr = asFilterExpression(mapper, attribute);
      if (attrFilterExpr != null) {
        query.withFilterExpression(attrFilterExpr);
      }

      query.withNameMap(mapper.nameMap);
      query.withValueMap(mapper.valueMap);

      Iterator iter = table.query(query).pages().iterator();
      return iter;
    }

    public Iterator<Page<Item, ?>> get() {
      GetItemSpec query = new GetItemSpec();
      query.withConsistentRead(consistentRead);
      if (!isStarQuery) {
        query.withProjectionExpression(projection);
      }
      DynamoReadFilterSpec filter = slice.getFilter();
      assert filter.getAttributeFilter() == null : "Gets cannot have an attribute filter!";
      // key space
      PrimaryKey pk = new PrimaryKey();
      DynamoFilterSpec key = filter.getKeyFilter();
      DynamoFilterSpec.FilterTree tree = key.getTree();
      DynamoFilterSpec.FilterNode node = tree.getRoot();

      // just a primary key
      if (node instanceof FilterLeaf) {
        assert tableDef.getKeys().size() == 1 : "Have more than 1 key, but only 1 key condition!";
        FilterLeaf hashLeaf = (FilterLeaf) node;
        setPrimaryKey(hashLeaf, pk);
      } else {
        // sort and hash key
        FilterNodeInner inner = (FilterNodeInner) node;
        setPrimaryKey((FilterLeaf) inner.getLeft(), pk);
        setPrimaryKey((FilterLeaf) inner.getRight(), pk);
      }
      query.withPrimaryKey(pk);

      return new AbstractIterator<Page<Item, ?>>() {
        private boolean ran = false;

        @Override
        protected Page<Item, ?> computeNext() {
          if (ran) {
            endOfData();
            return null;
          }
          try {
            Item i = table.getItem(query);
            return new GetItemPage(i);
          } finally {
            ran = true;
          }
        }
      };
    }

    private void setPrimaryKey(FilterLeaf leaf, PrimaryKey pk) {
      assert leaf.getOperand().equals("=") : "Gets must use '=' for attributes";
      pk.addComponent(leaf.getKey(), leaf.getValue());
    }
  }

  private String asFilterExpression(NameMapper mapper, DynamoFilterSpec spec) {
    if (spec == null || spec.getTree() == null) {
      return null;
    }
    DynamoFilterSpec.FilterTree tree = spec.getTree();
    // replace the leaf values in the tree with expressions
    List<DynamoFilterSpec.FilterNode> nodes = newLinkedList();
    return tree.visit(new DynamoFilterSpec.FilterNodeVisitor<String>() {
      @Override
      public String visitInnerNode(FilterNodeInner inner) {
        String left = inner.getLeft().visit(this);
        String right = inner.getRight().visit(this);

        return left + " " + inner.getCondition() + " " + right;
      }

      @Override
      public String visitLeafNode(FilterLeaf leaf) {
        String name = leaf.getKey();
        if (leaf.registerKey()) {
          name = mapper.name(name);
        }
        Object value = leaf.getValue();
        String valueName = null;
        if (leaf.registerValue()) {
          valueName = mapper.value(value);
        }
        return leaf.getOperand().compose(name, valueName);
      }
    });
  }

  public DynamoQueryBuilder withTable(DynamoTableDefinition tableDef) {
    this.tableDef = tableDef;
    return this;
  }

  private class NameMapper {
    private int counter = 0;
    Map<String, String> nameMap = new HashMap<>();
    Map<String, Object> valueMap = new HashMap<>();

    public String name(String name) {
      return add("#n", name, nameMap);
    }

    public String value(Object value) {
      return add(":v", value, valueMap);
    }

    private <IN extends Object> String add(String prefix, IN in, Map<String, IN> map) {
      String out = prefix + (counter++);
      map.put(out, in);
      return out;
    }
  }

  private class GetItemPage extends Page<Item, Item> {

    /**
     * @param item the item read
     */
    public GetItemPage(Item item) {
      super(unmodifiableList(singletonList(item)), item);
    }

    @Override
    public boolean hasNextPage() {
      return false;
    }

    @Override
    public Page<Item, Item> nextPage() {
      return null;
    }
  }
}
