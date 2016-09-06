package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.StoreClerk;
import org.apache.avro.Schema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/**
 * Base access for a logical Fineo table. This actually delegates to a series of unions to
 * underlying dynamo and/or spark tables, depending on the time range we are querying
 */
public class FineoTable extends DrillTable implements TranslatableTable {

  private static final Logger LOG = LoggerFactory.getLogger(FineoTable.class);
  private final SubTableScanBuilder scanner;
  private final StoreClerk.Metric metric;

  public FineoTable(FineoStoragePlugin plugin, String tableName,
    SubTableScanBuilder scanner, StoreClerk.Metric metric) {
    super(tableName, plugin, null, null);
    this.scanner = scanner;
    this.metric = metric;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    LogicalScanBuilder builder = new LogicalScanBuilder(context, relOptTable);
    try {
      scanner.scan(builder, metric.getMetricId());
    } catch (NullPointerException e) {
      // skip adding this scan - we couldn't find any files for this table & no dynamo
      // support...apparently.
      if (e.getMessage().startsWith("Could not find any input table from dfs")) {
        // remove the offending directory
        String pattern = "(.*(\\Q[\\E(.*)(\\Q]\\E\\z)))";
        Pattern pat = Pattern.compile(pattern);
        Matcher matcher = pat.matcher(e.getMessage());
        if(!matcher.matches()){
          LOG.error("Couldn't find any matching tables in '{}'", e.getMessage());
          throw e;
        }
        String fileString = matcher.group(3);
        for(String file: fileString.split(",")){
          file = file.replaceAll("\\s+","");
          builder.removeScan("dfs", file);
        }

        // create a temp json file with no data
        try {
          Path path = Files.createTempFile("fineo-empty-read", ".json");
          builder.scan("dfs", path.toString());
          scanner.scan(builder, metric.getMetricId());
        } catch (IOException e1) {
          LOG.error("Failed to create temp directoy file, just throwing original exception");
          throw e;
        }
      }else{
        throw e;
      }
    }
    return builder.buildMarker(this.metric);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
    // base fields
    boolean radio = ((FineoStoragePlugin) this.getPlugin()).getEnableRadio();
    for (BaseField field : BaseField.values()) {
      // skip the radio field if its not enabled at the system level
      if (field.getName().equals(FineoCommon.MAP_FIELD) && !radio) {
        continue;
      }
      field.add(builder, typeFactory);
    }

    // add all the user visible fields
    for (StoreClerk.Field field : this.metric.getUserVisibleFields()) {
      SqlTypeName type = getSqlType(field.getType());
      builder.add(field.getName(), type);
    }
    return builder.build();
  }

  private SqlTypeName getSqlType(Schema.Type type) {
    switch (type) {
      case STRING:
        return SqlTypeName.VARCHAR;
      case BOOLEAN:
        return SqlTypeName.BOOLEAN;
      case BYTES:
        return SqlTypeName.BINARY;
      case INT:
        return SqlTypeName.INTEGER;
      case LONG:
        return SqlTypeName.BIGINT;
      case FLOAT:
        return SqlTypeName.FLOAT;
      case DOUBLE:
        return SqlTypeName.DOUBLE;
      default:
        throw new IllegalArgumentException("We cannot type avro type: " + type);
    }
  }

  public enum BaseField {
    TIMESTAMP(AvroSchemaProperties.TIMESTAMP_KEY, tf -> tf.createSqlType(BIGINT)),
    RADIO(FineoCommon.MAP_FIELD,
      tf -> tf.createMapType(tf.createSqlType(VARCHAR), tf.createSqlType(ANY)));
    private final String name;
    private final Function<RelDataTypeFactory, RelDataType> func;

    BaseField(String name, Function<RelDataTypeFactory, RelDataType> func) {
      this.name = name;
      this.func = func;
    }

    public RelDataTypeFactory.FieldInfoBuilder add(RelDataTypeFactory.FieldInfoBuilder builder,
      RelDataTypeFactory factory) {
      return builder.add(name, func.apply(factory));
    }

    public String getName() {
      return name;
    }
  }
}
