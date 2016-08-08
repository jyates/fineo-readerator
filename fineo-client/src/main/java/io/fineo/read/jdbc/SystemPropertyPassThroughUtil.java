package io.fineo.read.jdbc;

import java.util.function.Consumer;

/**
 * Utility to pass jdbc properties around through system properties. Really only works because
 * if is only going to be one connection at a time being instantiated... otherwise, we need to
 * re-jigger how we manage the property passing in Avatica
 */
public class SystemPropertyPassThroughUtil {

  public static void set(FineoConnectionProperties prop, String value) {
    System.setProperty("fineo._" + prop.camelName(), value);
  }

  public static void set(FineoConnectionProperties prop, Consumer<String> consumer) {
    String property = get(prop);
    if (property == null) {
      return;
    }
    consumer.accept(property);
  }

  public static void setInt(FineoConnectionProperties prop, Consumer<Integer> consumer) {
    set(prop, str -> {
      Integer i = Integer.valueOf(str);
      if (i >= 0) {
        consumer.accept(i);
      }
    });
  }

  public static String get(FineoConnectionProperties prop) {
    return System.getProperty("fineo._" + prop.camelName());
  }
}
