package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import io.fineo.drill.exec.store.dynamo.config.ClientProperties;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;

import java.util.List;

public class DrillDynamoTable extends DynamicDrillTable {

  private final Table table;

  public DrillDynamoTable(DynamoStoragePlugin plugin, String
    tableName, ClientProperties clientProps, ParallelScanProperties scan) {
    super(plugin, tableName, new DynamoScanSpec(tableName, clientProps, scan));
    this.table = plugin.getModel().getTable(tableName);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType type = super.getRowType(typeFactory);
    // add the sort/partition keys that we know about
    TableDescription desc = table.getDescription();
    List<KeySchemaElement> keys = desc.getKeySchema();
    for (KeySchemaElement elem : keys) {
      type.getField(elem.getAttributeName(), true, false);
    }

    return type;
  }
}
