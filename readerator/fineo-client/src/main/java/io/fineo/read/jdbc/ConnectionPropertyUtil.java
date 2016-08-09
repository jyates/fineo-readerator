package io.fineo.read.jdbc;

import java.util.Map;
import java.util.function.Consumer;

public class ConnectionPropertyUtil {

  public static void set(Map<String, String> properties, FineoConnectionProperties prop,
    Consumer<String> consumer) {
    String property = properties.get(prop.camelName());
    if (property == null) {
      return;
    }
    consumer.accept(property);
  }

  public static void setInt(Map<String, String> properties, FineoConnectionProperties prop,
    Consumer<Integer> consumer) {
    set(properties, prop, str -> {
      Integer i = Integer.valueOf(str);
      if (i >= 0) {
        consumer.accept(i);
      }
    });
  }
}
