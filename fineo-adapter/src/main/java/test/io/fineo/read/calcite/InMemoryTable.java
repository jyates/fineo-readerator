package test.io.fineo.read.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;

/**
 *
 */
public class InMemoryTable extends AbstractTable implements ExtensibleTable {
  private List<RelDataTypeField> extensionFields = new ArrayList<>();

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

    // add the default fields that we expect from all users
    builder.add(ORG_ID_KEY.toUpperCase(), typeFactory.createSqlType(SqlTypeName.VARCHAR));

    for (RelDataTypeField field : extensionFields) {
      builder.add(field);
    }
    return builder.build();
  }

  @Override
  public Table extend(List<RelDataTypeField> fields) {
    this.extensionFields.addAll(fields);
    return this;
  }
}
