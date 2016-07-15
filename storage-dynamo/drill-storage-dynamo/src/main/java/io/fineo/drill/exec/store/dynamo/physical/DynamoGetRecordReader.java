package io.fineo.drill.exec.store.dynamo.physical;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Page;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubGetSpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubScanSpec;
import org.apache.drill.common.expression.SchemaPath;

import java.util.Iterator;
import java.util.List;

public class DynamoGetRecordReader extends DynamoRecordReader<DynamoSubGetSpec>{

  public DynamoGetRecordReader(AWSCredentialsProvider credentials, ClientConfiguration clientConf,
    DynamoEndpoint endpoint, DynamoSubGetSpec scanSpec,
    List<SchemaPath> columns, boolean consistentRead, DynamoTableDefinition scan) {
    super(credentials, clientConf, endpoint, scanSpec, columns, consistentRead, scan);
  }

  @Override
  protected Iterator<Page<Item, ?>> buildQuery(DynamoQueryBuilder builder,
    AmazonDynamoDBAsyncClient client) {
    return builder.build(client).get();
  }
}
