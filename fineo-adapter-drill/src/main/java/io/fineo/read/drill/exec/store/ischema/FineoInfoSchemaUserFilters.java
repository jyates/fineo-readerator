package io.fineo.read.drill.exec.store.ischema;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.typesafe.config.ConfigValueFactory.fromMap;

/**
 * Helper class to specify the Drill user filters
 */
public class FineoInfoSchemaUserFilters {

  static final String FINEO_HIDDEN_USER_NAME = "fineo_internal_hidden_chunky_monkey";

  public static DrillConfig overrideWithInfoSchemaFilters(DrillConfig conf){
    // translator ensures that we hide information from the user
    Map<String, String> translator = new HashMap<>();
    translator.put("@class", FineoInfoSchemaUserTranslator.class.getName());
    conf = new DrillConfig(conf.withValue(ExecConstants.ISCHEMA_TRANSLATE_RULES_KEY,
      fromMap(translator)), false);

    Map<String, Map<String, Collection<String>>> filters = new HashMap<>();
    Map<String, Collection<String>> userSchemaMap = new HashMap<>();
    // users can only see their own schemas, which get translated to a hidden value
    userSchemaMap.put("^(?!anonymous).*$", newArrayList("INFORMATION_SCHEMA", "FINEO"));
    // anonymous cannot see anything
    userSchemaMap.put("anonymous", newArrayList(""));
    // fineo-internal can see all the table definitions as they are specified (partially implemented
    // in the translator as well)
    userSchemaMap.put(FINEO_HIDDEN_USER_NAME, newArrayList("%"));
    filters.put("schemas", userSchemaMap);

    return new DrillConfig(
      conf.withValue(ExecConstants.PER_USER_ISCHEMA_FILTER_RULES_KEY, fromMap(filters)), false);
  }
}
