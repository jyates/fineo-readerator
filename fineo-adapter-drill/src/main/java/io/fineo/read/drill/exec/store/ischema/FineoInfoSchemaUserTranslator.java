package io.fineo.read.drill.exec.store.ischema;

import org.apache.drill.exec.store.ischema.InfoSchemaConstants;
import org.apache.drill.exec.store.ischema.InfoSchemaTranslator;
import org.apache.drill.exec.store.ischema.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.fineo.read.drill.exec.store.ischema.FineoInfoSchemaUserFilters
  .FINEO_HIDDEN_USER_NAME;

/**
 * Translate reads of the information schema table to use the translator instead.
 * <p>
 * Only exclusion is our internal user which gets to see all the tables as they are defined
 * </p>
 */
public class FineoInfoSchemaUserTranslator extends InfoSchemaTranslator {

  private static final Logger LOG =
    LoggerFactory.getLogger(FineoInfoSchemaUserTranslator.class);
  private static final String FINEO = "FINEO";
  public static final String FINEO_DESCRIPTION = "Tables hosted by Fineo";
  public static final String INFO_SCHEMA = "INFORMATION_SCHEMA";

  @Override
  protected Records.Catalog handleCatalog(Records.Catalog input) {
    if (user.equals(FINEO_HIDDEN_USER_NAME)) {
      return input;
    }
    return new Records.Catalog(FINEO, FINEO_DESCRIPTION, "");
  }

  @Override
  protected Records.Schema handleSchema(Records.Schema input) {
    if (user.equals(FINEO_HIDDEN_USER_NAME)) {
      return input;
    }
    if (input.SCHEMA_NAME.equals(INFO_SCHEMA)) {
      return new Records.Schema(FINEO, input.SCHEMA_NAME, "system", input.TYPE, false);
    }

    // switch for the fineo error records
    String[] parts = input.SCHEMA_NAME.split("[.]");
    if(parts.length == 0){
      throw new IllegalStateException("Got an internal schema without a separator!");
    }
    String name = parts[1].equals("errors")? "errors": parts[0];
    return new Records.Schema(FINEO, name.toUpperCase(), "user", FINEO,
      input.IS_MUTABLE.equals("YES"));
  }

  @Override
  protected Records.Table handleTable(Records.Table input) {
    if (user.equals(FINEO_HIDDEN_USER_NAME)) {
      return input;
    }
    if (input.TABLE_SCHEMA.equals(INFO_SCHEMA)) {
      return new Records.Table(FINEO, input.TABLE_SCHEMA, input.TABLE_NAME, input.TABLE_TYPE);
    }

    // schema gets overridden, but table name doesn't change
    return new Records.Table(FINEO, FINEO, input.TABLE_NAME, input.TABLE_TYPE);
  }

  @Override
  protected Records.Column handleColumn(Records.Column input) {
    return new Records.Column(FINEO, FINEO, input.TABLE_NAME, input);
  }

  @Override
  protected Map<String, String> handleMap(Map input) {
    Map<String, String> output = input;
    output = putIfNotNull(output, InfoSchemaConstants.SCHS_COL_CATALOG_NAME, (value) -> "DRILL");
    output = putIfNotNull(output, InfoSchemaConstants.SCHS_COL_SCHEMA_NAME, schemaConversion);
    output = putIfNotNull(output, InfoSchemaConstants.SHRD_COL_TABLE_SCHEMA, schemaConversion);
    output = putIfNotNull(output, InfoSchemaConstants.SHRD_COL_TABLE_NAME, Function.identity());
    return output;
  }

  private Function<String, String> schemaConversion = (value) -> {
    if (value.equals("fineo." + user)) {
      return "FINEO";
    } else if(value.equals("fineo.errors.default")){
      return "ERRORS";
    } else if (value.equals(InfoSchemaConstants.IS_SCHEMA_NAME)) {
      return value;
    } else {
      return "_SKIP";
    }
  };

  private Map<String, String> putIfNotNull(Map<String, String> map, String key,
    Function<String, String> transform) {
    String value = map.get(key);
    // information schema is returned as-is
    if (value != null) {
      Map<String, String> hash = new HashMap<>(map);
      map = hash;
      map.put(key, transform.apply(value));
    }
    return map;
  }
}
