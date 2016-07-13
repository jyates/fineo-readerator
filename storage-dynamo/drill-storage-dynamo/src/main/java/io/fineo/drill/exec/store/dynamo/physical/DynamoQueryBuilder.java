package io.fineo.drill.exec.store.dynamo.physical;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import io.fineo.drill.exec.store.dynamo.spec.DynamoScanFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;

import java.util.Iterator;
import java.util.List;

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
    if (filters.getHashKeyFilter() != null) {
      if (tableDef.getKeys().size() == 1 || filters.getRangeKeyFilter() != null) {
        return buildGet(table);
      } else {
        return buildQuery(table);
      }
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

  private Iterator<Page<Item, ?>> buildQuery(Table table) {
    return null;
  }

  private Iterator<Page<Item, ?>> buildGet(Table table) {
    return null;
  }
}
