package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
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

import static com.google.common.collect.Lists.newArrayList;

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
    List<KeySchemaElement> keys = desc.getKeySchema();
    List<AttributeDefinition> attributes = desc.getAttributeDefinitions();
    Map<String, DynamoTableDefinition.PrimaryKey> map = new HashMap<>();
    for(KeySchemaElement key: keys){
      DynamoTableDefinition.PrimaryKey pk = new DynamoTableDefinition.PrimaryKey(key
        .getAttributeName(), null, KeyType.valueOf(key.getKeyType()) == KeyType.HASH);
      map.put(key.getAttributeName(), pk);
    }
    for (AttributeDefinition elem :  attributes){
      map.get(elem.getAttributeName()).setType(elem.getAttributeType());
    }
    List<DynamoTableDefinition.PrimaryKey> pks = newArrayList(map.values());
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
