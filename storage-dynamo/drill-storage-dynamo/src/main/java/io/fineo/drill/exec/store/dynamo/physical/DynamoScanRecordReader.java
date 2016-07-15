package io.fineo.drill.exec.store.dynamo.physical;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Page;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubScanSpec;
import org.apache.drill.common.expression.SchemaPath;

import java.util.Iterator;
import java.util.List;

/**
 * Actually do the get/query/scan based on the {@link DynamoSubScanSpec}.
 */
public class DynamoScanRecordReader extends DynamoRecordReader<DynamoSubScanSpec>{

  private final ParallelScanProperties scanProps;

  public DynamoScanRecordReader(AWSCredentialsProvider credentials, ClientConfiguration clientConf,
    DynamoEndpoint endpoint, DynamoSubScanSpec scanSpec,
    List<SchemaPath> columns, boolean consistentRead, ParallelScanProperties scanProperties,
    DynamoTableDefinition scan) {
    super(credentials, clientConf, endpoint, scanSpec, columns, consistentRead, scan);
    this.scanProps = scanProperties;
  }

  @Override
  protected Iterator<Page<Item, ?>> buildQuery(DynamoQueryBuilder builder,
    AmazonDynamoDBAsyncClient client) {
    return builder.withProps(scanProps).build(client).scan();
  }
}
