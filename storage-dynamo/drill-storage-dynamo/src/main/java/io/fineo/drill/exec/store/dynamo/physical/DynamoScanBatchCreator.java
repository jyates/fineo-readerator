package io.fineo.drill.exec.store.dynamo.physical;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.fineo.drill.exec.store.dynamo.config.ClientProperties;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubGetSpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubQuerySpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubReadSpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubScan;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubScanSpec;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

import java.util.List;

// Created by reflection by drill to match the DynamoSubScan
@SuppressWarnings("unused")
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

    for (DynamoSubReadSpec scanSpec : subScan.getSpecs()) {
      try {
        if (scanSpec instanceof DynamoSubGetSpec) {
          readers.add(new DynamoGetRecordReader(credentials, client, endpoint,
            (DynamoSubGetSpec) scanSpec, columns,
            clientProps.getConsistentRead(), subScan.getTable()));
        } else if (scanSpec instanceof DynamoSubQuerySpec) {
          readers.add(new DynamoQueryRecordReader(credentials, client, endpoint,
            (DynamoSubQuerySpec) scanSpec, columns,
            clientProps.getConsistentRead(), subScan.getScanProps(), subScan.getTable()));
        } else {
          readers.add(new DynamoScanRecordReader(credentials, client, endpoint,
            (DynamoSubScanSpec) scanSpec, columns,
            clientProps.getConsistentRead(), subScan.getScanProps(), subScan.getTable()));
        }
      } catch (Exception e1) {
        throw new ExecutionSetupException(e1);
      }
    }
    return new ScanBatch(subScan, context, readers.iterator());
  }
}