package io.fineo.read.drill.exec.store.schema;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.plugin.source.DynamoSourceTable;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.read.drill.exec.store.plugin.source.SourceTable;
import io.fineo.schema.avro.AvroSchemaEncoder;
import oadd.com.google.common.base.Joiner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class SubTableScanBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SubTableScanBuilder.class);
  private static final Joiner PATH = Joiner.on("/");
  private final Multimap<String, SourceTable> sources = ArrayListMultimap.create();
  private final String org;
  private final DynamoDB dynamo;

  public SubTableScanBuilder(String org, Set<SourceTable> sourceFsTables, DynamoDB dynamo) {
    this.org = org;
    for (SourceTable source : sourceFsTables) {
      sources.put(source.getSchema(), source);
    }
    this.dynamo = dynamo;
  }

  public void scan(LogicalScanBuilder builder, String metricId) {
    for (Map.Entry<String, SourceTable> schemas : sources.entries()) {
      switch (schemas.getKey()) {
        case "dfs":
          String baseDir = getBaseDir((FsSourceTable) schemas.getValue(), metricId);
          builder.scan(schemas.getKey(), baseDir);
          break;
        case "dynamo":
          DynamoSourceTable source = (DynamoSourceTable) schemas.getValue();
          String prefix = source.getPrefix();
          String regex = source.getPattern();
          Pattern pattern = Pattern.compile(regex);
          for (Table table : dynamo.listTables(prefix)) {
            String name = table.getTableName();
            // moved off the prefix
            if (!name.startsWith(prefix)) {
              break;
            }

            if (pattern.matcher(name).matches()) {
              builder.scan(addDynamoScan(builder, name, metricId));
            } else {
              LOG.info("Skipping non-matching dynamo table: {}", name);
            }
          }
      }
    }
  }

  private RelNode addDynamoScan(LogicalScanBuilder builder, String tableName, String metricId) {
    return builder.getTableScan("dynamo", tableName);
    // we already filter on org/metric in the recombinator, so no need to add it here a second time
//    // add a filter for the table on the org and metric
//    RelBuilder rel = builder.getRelBuilder();
//    rel.push(scan);
//
//    List<RexNode> conditions = new ArrayList<>(2);
//    conditions.add(equals(scan, AvroSchemaEncoder.ORG_ID_KEY, org));
//    conditions.add(equals(scan, AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metricId));
//
//    return rel.filter(conditions).build();
  }

  private RexNode equals(LogicalTableScan scan, String field, String value) {
    RexBuilder builder = scan.getCluster().getRexBuilder();
    RexNode litValue =
      builder.makeLiteral(value, builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), true);
    RelDataType table = scan.getRowType();
    int index = table.getField(field, true, false).getIndex();
    RexNode ref = builder.makeInputRef(scan, index);
    return builder.makeCall(SqlStdOperatorTable.EQUALS, ref, litValue);
  }

  private String getBaseDir(FsSourceTable table, String metricId) {
    //TODO replace this with a common "directory layout" across this and the BatchETL job.
    // Right now this is an implicit linkage and it would be nicer to formalize that (and add
    // safety by checking version)
    return PATH
      .join(table.getBasedir(), FineoStoragePlugin.VERSION, table.getFormat(), org, metricId);
  }
}
