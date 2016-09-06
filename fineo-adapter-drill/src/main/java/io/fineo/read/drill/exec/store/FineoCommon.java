package io.fineo.read.drill.exec.store;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.store.AvroSchemaProperties.ORG_ID_KEY;
import static io.fineo.schema.store.AvroSchemaProperties.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.store.AvroSchemaProperties.TIMESTAMP_KEY;

/**
 *
 */
public class FineoCommon {
  public static final List<String> REQUIRED_FIELDS =
    newArrayList(ORG_ID_KEY, ORG_METRIC_TYPE_KEY, TIMESTAMP_KEY);

  /**
   * This is also overridden in resources/drill-module.conf
   */
  public static final String PARTITION_DESIGNATOR = "_fd";

  public static final String MAP_FIELD = "_fm";
  public static boolean RADIO_ENABLED_TEST_OVERRIDE = false;
  public static boolean isRadioEnabled() {
    return RADIO_ENABLED_TEST_OVERRIDE;
  }
}
