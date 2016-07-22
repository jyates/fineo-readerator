package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
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
  private final DynamoKeyMapperSpec key;

  public DrillDynamoTable(DynamoStoragePlugin plugin, String tableName,
    DynamoKeyMapperSpec keyMapper) {
    super(plugin, tableName, new DynamoGroupScanSpec());
    try {
      this.desc = plugin.getModel().getTable(tableName).waitForActive();
    } catch (InterruptedException e) {
      throw new DrillRuntimeException(e);
    }
    this.key = keyMapper;

    DynamoGroupScanSpec spec = ((DynamoGroupScanSpec) this.getSelection());
    // figure out the pk map
    List<KeySchemaElement> keys = desc.getKeySchema();
    List<AttributeDefinition> attributes = desc.getAttributeDefinitions();
    Map<String, DynamoTableDefinition.PrimaryKey> map = new HashMap<>();
    for (KeySchemaElement key : keys) {
      DynamoTableDefinition.PrimaryKey pk = new DynamoTableDefinition.PrimaryKey(key
        .getAttributeName(), null, KeyType.valueOf(key.getKeyType()) == KeyType.HASH);
      map.put(key.getAttributeName(), pk);
    }
    for (AttributeDefinition elem : attributes) {
      map.get(elem.getAttributeName()).setType(elem.getAttributeType());
    }
    List<DynamoTableDefinition.PrimaryKey> pks = newArrayList(map.values());
    DynamoTableDefinition def = new DynamoTableDefinition(tableName, pks, keyMapper);
    spec.setTable(def);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType type = super.getRowType(typeFactory);
    // add the sort/partition keys that the mapper should produce
    if (key != null) {
      for (String field : key.getKeyNames()) {
        type.getField(field, true, false);
      }
    } else {
      // no mapper, so just rely on what the descriptor says
      List<KeySchemaElement> keys = desc.getKeySchema();
      for (KeySchemaElement elem : keys) {
        type.getField(elem.getAttributeName(), true, false);
      }
    }

    return type;
  }
}
