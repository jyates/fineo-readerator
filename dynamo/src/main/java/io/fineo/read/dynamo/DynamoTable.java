package io.fineo.read.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import io.fineo.lambda.dynamo.ResultOrException;
import io.fineo.lambda.dynamo.iter.PageManager;
import io.fineo.lambda.dynamo.iter.PagingIterator;
import io.fineo.lambda.dynamo.iter.ScanPager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Stand-in for the calcite implementation of Dynamo reading... whenever that shows up
 */
public class DynamoTable {

  private final AmazonDynamoDBAsyncClient client;
  private final String partitionKey;

  public DynamoTable(AmazonDynamoDBAsyncClient client, String partitionKey){
    this.client = client;
    this.partitionKey = partitionKey;
  }

  public Iterable<ResultOrException<Map<String, AttributeValue>>> query(
    List<Map.Entry<String, String>> selectFields, String limit) {
    ScanRequest scan = new ScanRequest();
    if (limit != null) {
      scan.setLimit(Integer.valueOf(limit));
    }
    if (!selectFields.isEmpty()) {
      Iterator<String> projections =
        Iterators.transform(selectFields.iterator(), field -> field.getKey());
      scan.setProjectionExpression(Joiner.on(' ').join(projections));
    }

    // TODO support filter expressions

    List<ScanPager> scanners = new ArrayList<>(1);
    scanners.add(new ScanPager(client, scan, partitionKey, "org_metric0"));

    return () -> new PagingIterator<>(1, new PageManager(scanners));
  }
}
