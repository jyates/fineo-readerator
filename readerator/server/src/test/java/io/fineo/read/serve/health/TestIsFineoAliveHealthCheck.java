package io.fineo.read.serve.health;

import io.fineo.read.serve.TestFineoMeta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.read.serve.TestBaseInternalHandler.part;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

public class TestIsFineoAliveHealthCheck {

  @Test
  public void testCheckMatches() throws Exception {
    JdbcMeta meta = TestFineoMeta.getMeta();
    IsFineoAliveCheck check = new IsFineoAliveCheck(meta);
    assertTrue(check.matches(newArrayList(part("alive"), part("fineo"))));
    assertFalse(check.matches(newArrayList(part("alive"))));
    assertFalse(check.matches(newArrayList()));
    assertFalse(check.matches(newArrayList(part("alive"), part("fineo"), part("fineo"))));
  }

  @Test
  public void testCheckAlive() throws Exception {
    JdbcMeta meta = TestFineoMeta.getMeta();
    IsFineoAliveCheck check = new IsFineoAliveCheck(meta);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    check.handle("target", null, null, response);
    Mockito.verify(response).setStatus(200);
    Mockito.verify(response).setContentLength(0);
  }

  @Test
  public void testReturnsErrorIfNoConnection() throws Exception {
    JdbcMeta meta = Mockito.mock(JdbcMeta.class);
    Mockito.when(meta.getCatalogs(any())).thenThrow(new RuntimeException("Injected failure"));
    IsFineoAliveCheck check = new IsFineoAliveCheck(meta);
    WriteCheckResponseStream stream = new WriteCheckResponseStream();
    HttpServletResponse response = mockResponse(stream);
    check.handle("target", null, null, response);
    verifyFailed(response, stream);
  }

  public static HttpServletResponse mockResponse(ServletOutputStream stream) throws IOException {
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(response.getOutputStream()).thenReturn(stream);
    return response;
  }

  public static void verifyFailed(HttpServletResponse mock, WriteCheckResponseStream stream) {
    Mockito.verify(mock).setStatus(500);
    Mockito.verify(mock).setContentLength(Mockito.anyInt());
    assertTrue("Threw error, but didn't write an error message", stream.wrote());
  }

  public static class WriteCheckResponseStream extends ServletOutputStream {
    boolean wrote[] = new boolean[1];

    @Override
    public void write(int b) throws IOException {
      wrote[0] = true;
    }

    public boolean wrote() {
      return wrote[0];
    }
  }
}
