package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.fineo.drill.exec.store.dynamo.DynamoGroupScan;
import io.fineo.drill.exec.store.dynamo.DynamoPlanValidationUtils;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.avro.AvroFormatConfig;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.NamedFormatPluginConfig;
import org.apache.drill.exec.store.easy.json.JSONFormatPlugin;
import org.apache.drill.exec.store.easy.sequencefile.SequenceFileFormatConfig;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PlanValidator {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  static {
    MAPPER.registerSubtypes(FileSystemConfig.class);
    // all the different formats, which the EasyScan plugin serializes, for some reason
    MAPPER.registerSubtypes(JSONFormatPlugin.JSONFormatConfig.class,
      AvroFormatConfig.class,
      TextFormatPlugin.TextFormatConfig.class,
      NamedFormatPluginConfig.class,
      ParquetFormatConfig.class,
      SequenceFileFormatConfig.class
    );
    // SchemaPath doesn't have a good deserializer for some reason...
    MAPPER.registerSubtypes(FieldReference.class);
    SimpleModule module = new SimpleModule("schema-path-deserializer");
    module.addDeserializer(SchemaPath.class, new JsonDeserializer<SchemaPath>() {
      @Override
      public SchemaPath deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
        return SchemaPath.create(UserBitShared.NamePart.newBuilder().setName(p.getText()).build());
      }
    });
    MAPPER.registerModule(module);
  }

  private final String query;
  private Map<String, BaseValidator> validators = new HashMap<>();

  public PlanValidator(String query) {
    this.query = query;
  }

  public DynamoValidator validateDynamoQuery() {
    DynamoValidator validator = new DynamoValidator();
    validators.put(validator.getPop(), validator);
    return validator;
  }

  public void validate(Connection conn) throws SQLException, IOException {
    String explain = explain(query);
    ResultSet plan = conn.createStatement().executeQuery(explain);
    assertTrue("After successful read, could not get the plan for query: " + explain, plan.next());
    String jsonPlan = plan.getString("json");
    Map<String, Object> jsonMap = MAPPER.readValue(jsonPlan, Map.class);
    List<Map<String, Object>> graph = (List<Map<String, Object>>) jsonMap.get("graph");
    for (Map.Entry<String, BaseValidator> validator : validators.entrySet()) {
      Map<String, Object> scan = getGraphStep(graph, validator.getValue().getPop());
      validator.getValue().accept(scan);
    }
  }

  private Map<String, Object> getGraphStep(List<Map<String, Object>> graph, String popName) {
    for (Map<String, Object> pop : graph) {
      if (pop.get("pop").equals(popName)) {
        return pop;
      }
    }
    return null;
  }

  public ParquetValidator validateParquetScan() {
    ParquetValidator validator = new ParquetValidator();
    validators.put(validator.getPop(), validator);
    return validator;
  }

  private abstract class BaseValidator<T extends BaseValidator> implements
                                                                Consumer<Map<String, Object>> {
    private final String pop;
    protected List<String> columns;

    public BaseValidator(String pop) {
      this.pop = pop;
    }

    public T withColumns(String... columns) {
      this.columns = Arrays.asList(columns).stream().map(FineoTestUtil::bt).collect(Collectors
        .toList());
      return (T) this;
    }

    protected void setColumns() {
      if (columns == null) {
        columns = newArrayList("`*`");
      }
    }

    public PlanValidator done() {
      return PlanValidator.this;
    }

    public String getPop() {
      return pop;
    }
  }

  public class ParquetValidator extends BaseValidator<ParquetValidator> {

    private File selectionRoot;
    private List<String> files;
    private final String filePrefix = "file:";
    private Class<? extends FormatPluginConfig> pluginFormat;

    public ParquetValidator() {
      super("parquet-scan");
    }

    public ParquetValidator withFormat(Class<? extends FormatPluginConfig> format) {
      this.pluginFormat = format;
      return this;
    }

    public ParquetValidator withSelectionRoot(File selectionRoot) {
      this.selectionRoot = selectionRoot;
      return this;
    }

    public ParquetValidator withFiles(List<File> files) {
      this.files = files.stream().map(File::toString).collect(Collectors.toList());
      return this;
    }

    @Override
    public void accept(Map<String, Object> scan) {
      setColumns();
      assertEquals(columns, scan.get("columns"));
      assertEquals(files, scan.get("files"));
      assertEquals(filePrefix + selectionRoot, scan.get("selectionRoot"));
      try {
        FormatPluginConfig format = MAPPER.readValue(MAPPER.writeValueAsString(scan.get("format")),
          FormatPluginConfig.class);
        assertEquals(this.pluginFormat, format.getClass());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public class DynamoValidator extends BaseValidator<DynamoValidator> {
    private String table;
    private DynamoReadFilterSpec scan;
    private List<DynamoReadFilterSpec> getOrQuery;

    public DynamoValidator() {
      super(DynamoGroupScan.NAME);
    }

    public DynamoValidator withTable(Table table) {
      this.table = table.getTableName();
      return this;
    }

    public DynamoValidator withScan(DynamoReadFilterSpec scan) {
      this.scan = scan;
      return this;
    }

    public DynamoValidator withGetOrQueries(DynamoReadFilterSpec... getOrQuery) {
      this.getOrQuery = newArrayList(getOrQuery);
      return this;
    }

    @Override
    public void accept(Map<String, Object> scan) {
      setColumns();
      try {
        DynamoGroupScanSpec spec = DynamoPlanValidationUtils.validatePlan(scan, columns, this.scan,
          getOrQuery);
        DynamoTableDefinition tableDef = spec.getTable();
        assertEquals(table, tableDef.getName());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static File getSelectionRoot(SchemaStore store, FsSourceTable source, String org,
    String metrictype) throws SchemaNotFoundException {
    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metrictype);
    File selectionRoot = new File(source.getBasedir(), "0");
    selectionRoot = new File(selectionRoot, source.getFormat());
    selectionRoot = new File(selectionRoot, org);
    return new File(selectionRoot, metric.getMetricId());
  }

  private String explain(String sql) {
    return "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITH IMPLEMENTATION FOR " + sql;
  }
}
