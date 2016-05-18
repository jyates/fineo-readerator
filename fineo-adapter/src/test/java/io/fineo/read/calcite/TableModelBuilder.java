package io.fineo.read.calcite;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
    + "      dynamo: {\n"
    + "        url: '%s'\n"
    + "      },\n"
    + "      repository: {\n"
    + "        table: '%s'\n"
    + "      }\n"
    + "    }\n"
    + "  }\n";


  private final Map<String, String> props;
  private String dynamoUrl;
  private String schemaTable;


  public TableModelBuilder() {
    this.props = new HashMap<>();
  }

  public TableModelBuilder setDynamo(String url) {
    this.dynamoUrl = url;
    return this;
  }

  public TableModelBuilder setSchemaTable(String table) {
    this.schemaTable = table;
    return this;
  }

  public Map<String, String> build() {
    String schema = String.format(FINEO_SCHEMA, dynamoUrl, schemaTable);
    props.put("model", "inline:"+ String.format(FINEO_MODEL, schema));

    return props;
  }

  public static String quote(String s) {
    return "\"" + s + "\"";
  }
}
