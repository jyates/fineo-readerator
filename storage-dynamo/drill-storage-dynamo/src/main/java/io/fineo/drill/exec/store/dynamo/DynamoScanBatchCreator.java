package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.fineo.drill.exec.store.dynamo.config.ClientProperties;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

import java.util.List;

public class DynamoScanBatchCreator implements BatchCreator<DynamoSubScan> {
  @Override
  public CloseableRecordBatch getBatch(FragmentContext context, DynamoSubScan subScan,
    List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    List<RecordReader> readers = Lists.newArrayList();
    List<SchemaPath> columns = subScan.getColumns();
    ClientProperties clientProps = subScan.getClient();
    ClientConfiguration client = clientProps.getConfiguration();
    AWSCredentialsProvider credentials = subScan.getCredentials();
    DynamoEndpoint endpoint = subScan.getEndpoint();

    for (DynamoSubScan.DynamoSubScanSpec scanSpec : subScan.getSpecs()) {
      try {
        readers.add(new DynamoRecordReader(credentials, client, endpoint, scanSpec, columns,
          clientProps.getConsistentRead()));
      } catch (Exception e1) {
        throw new ExecutionSetupException(e1);
      }
    }
    return new ScanBatch(subScan, context, readers.iterator());
  }
}
