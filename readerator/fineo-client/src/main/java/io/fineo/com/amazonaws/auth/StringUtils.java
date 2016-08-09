package io.fineo.com.amazonaws.auth;

/**
 *
 */
public class StringUtils {

  /**
   * A null-safe trim method. If the input string is null, returns null;
   * otherwise returns a trimmed version of the input.
   */
  public static String trim(String value) {
    if (value == null) {
      return null;
    }
    return value.trim();
  }

  /**
   * @return true if the given value is either null or the empty string
   */
  public static boolean isNullOrEmpty(String value) {
    if (value == null) {
      return true;
    }
    return value.isEmpty();
  }

}
