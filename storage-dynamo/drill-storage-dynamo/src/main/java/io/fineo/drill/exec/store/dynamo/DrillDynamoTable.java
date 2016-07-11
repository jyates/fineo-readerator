package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import io.fineo.drill.exec.store.dynamo.config.ClientProperties;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DrillDynamoTable extends DynamicDrillTable {

  private final TableDescription desc;

  public DrillDynamoTable(DynamoStoragePlugin plugin, String
    tableName, ClientProperties clientProps, ParallelScanProperties scan) {
    super(plugin, tableName, new DynamoScanSpec());
    try {
      this.desc = plugin.getModel().getTable(tableName).waitForActive();
    } catch (InterruptedException e) {
      throw new DrillRuntimeException(e);
    }

    DynamoScanSpec spec = ((DynamoScanSpec) this.getSelection());
    spec.setClient(clientProps);
    spec.setScan(scan);

    // figure out the pk map
    Map<String, String> pks = new HashMap<>();
    for (AttributeDefinition elem : desc.getAttributeDefinitions()) {
      pks.put(elem.getAttributeName(), elem.getAttributeType());
    }
    DynamoTableDefinition def = new DynamoTableDefinition(tableName, pks);
    spec.setTable(def);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType type = super.getRowType(typeFactory);
    // add the sort/partition keys that we know about
    List<KeySchemaElement> keys = desc.getKeySchema();
    for (KeySchemaElement elem : keys) {
      type.getField(elem.getAttributeName(), true, false);
    }

    return type;
  }
}
