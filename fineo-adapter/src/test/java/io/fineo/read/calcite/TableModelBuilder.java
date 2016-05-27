package io.fineo.read.calcite;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class TableModelBuilder {

  private static final String FINEO_MODEL = "{\n"
                                            + "  version: '1.0',\n"
                                            + "  defaultSchema: 'FINEO',\n"
                                            + "   schemas: [\n"
                                            + "%s"
                                            + "   ]\n"
                                            + "}";
  private static String FINEO_SCHEMA =
    "  {\n"
    + "    name: 'FINEO',\n"
    + "    type: 'custom',\n"
    + "    factory: '" + FineoLocalSchemaFactory.class.getName() + "',\n"
    + "    operand: {\n"
    + "      repository: {\n"
    + "        table: '%s'\n"
    + "      }\n"
    + "      dynamo: {\n"
    + "        url: '%s'\n"
    + "      },\n"
    + "    }\n"
    + "  }\n";

  private static String FINEO_CSV_SCHEMA =
    "  {\n"
    + "    name: 'FINEO',\n"
    + "    type: 'custom',\n"
    + "    factory: '" + FineoLocalSchemaFactory.class.getName() + "',\n"
    + "    operand: {\n"
    + "      csv-schema: {\n"
    + "        directory: csv-test'\n"
    + "      },\n"
    + "      repository: {\n"
    + "        table: '%s'\n"
    + "      },\n"
    + "      dynamo: {\n"
    + "        url: '%s'\n"
    + "      },\n"
    + "    }\n"
    + "  }\n";


  private final Map<String, String> props;
  private String dynamoUrl;
  private String schemaTable;
  private boolean csv;


  public TableModelBuilder() {
    this.props = new HashMap<>();
  }

  public TableModelBuilder setDynamo(String url) {
    this.dynamoUrl = url;
    return this;
  }

  public TableModelBuilder useCsv() {
    this.csv = true;
    return this;
  }

  public TableModelBuilder setSchemaTable(String table) {
    this.schemaTable = table;
    return this;
  }

  public Map<String, String> build() {
    String base = csv ? FINEO_CSV_SCHEMA : FINEO_SCHEMA;
    String schema = String.format(base, schemaTable, dynamoUrl);
    props.put("model", "inline:" + String.format(FINEO_MODEL, schema));

    return props;
  }

  public static String quote(String s) {
    return "\"" + s + "\"";
  }
}
