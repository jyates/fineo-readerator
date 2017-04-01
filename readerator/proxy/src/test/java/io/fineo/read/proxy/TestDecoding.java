package io.fineo.read.proxy;

import org.junit.Test;

import java.net.URLDecoder;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * Ensure that we decode requests correctly
 */
public class TestDecoding {

  @Test
  public void testUrlDecoding() throws Exception {
    String expected = "SELECT type, stage, event, message, handled_timestamp FROM errors.stream  "
                      + "WHERE (stage = 'raw' OR stage = 'staged') AND (type = 'error' OR type = "
                      + "'malformed') AND `timestamp` BETWEEN 0 AND 1491004241121 AND (CAST"
                      + "(`year` AS INT) BETWEEN 1970 AND 2017) AND (CAST(`month` AS INT) BETWEEN"
                      + " 0 AND 2) AND (CAST(`day` AS INT) BETWEEN 4 AND 5) AND (CAST(`hour` AS "
                      + "INT) BETWEEN 0 AND 23) LIMIT 500";
    String query = "SELECT%20type%2C%20stage%2C%20event%2C%20message%2C%20handled_timestamp"
                   + "%20FROM%20errors.stream%20%20WHERE%20"
                   + "(stage%20%3D%20%27raw%27%20OR%20stage%20%3D%20%27staged%27)%20AND%20"
                   + "(type%20%3D%20%27error%27%20OR%20type%20%3D%20%27malformed%27)"
                   + "%20AND%20%60timestamp%60%20BETWEEN%200%20AND%201491004241121%20AND%20(CAST"
                   + "(%60year%60%20AS%20INT)%20BETWEEN%201970%20AND%202017)%20AND%20(CAST"
                   + "(%60month%60%20AS%20INT)%20BETWEEN%200%20AND%202)%20AND%20(CAST"
                   + "(%60day%60%20AS%20INT)%20BETWEEN%204%20AND%205)%20AND%20(CAST"
                   + "(%60hour%60%20AS%20INT)%20BETWEEN%200%20AND%2023)%20LIMIT%20500";
    String converted = JdbcHandler.decode(query);
    assertEquals(expected, converted);
    assertEquals(expected, JdbcHandler.decode(converted));
  }

  @Test
  public void testRemoveExtraQuoting() throws Exception {
    String actual = "SELECT * FROM errors.stream";
    String send = format("\"%s\"", actual);
    assertEquals(actual, JdbcHandler.decode(send));
    assertEquals(actual, JdbcHandler.decode(actual));
  }
}
