package io.fineo.read.serve.health;

import io.fineo.read.serve.TestFineoMeta;
import io.fineo.read.serve.driver.FineoDatabaseMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletResponse;

import static io.fineo.read.serve.health.TestIsFineoAliveHealthCheck.mockResponse;
import static io.fineo.read.serve.health.TestIsFineoAliveHealthCheck.verifyFailed;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isNull;

public class TestIsDrillAliveHealthCheck {

  @Test
  public void testCheckMatches() throws Exception {
    JdbcMeta meta = TestFineoMeta.getMeta();
    IsDrillAliveCheck check = new IsDrillAliveCheck(meta);
    assertTrue(check.matches(new String[]{"alive", "drill"}));
    assertFalse(check.matches(new String[]{"alive"}));
    assertFalse(check.matches(new String[]{}));
    assertFalse(check.matches(new String[]{"alive", "drill", "drill"}));
  }

  @Test
  public void testReturnsErrorIfNoConnection() throws Exception {
    JdbcMeta meta = Mockito.mock(JdbcMeta.class);
    Mockito.when(meta.getTables(any(), Mockito.eq(FineoDatabaseMetaData.FINEO_CATALOG),
      isNull(Meta.Pat.class), isNull(Meta.Pat.class), Mockito.anyList()))
           .thenThrow(new RuntimeException("Injected failure"));
    IsDrillAliveCheck check = new IsDrillAliveCheck(meta);
    TestIsFineoAliveHealthCheck.WriteCheckResponseStream
      stream = new TestIsFineoAliveHealthCheck.WriteCheckResponseStream();
    HttpServletResponse response = mockResponse(stream);
    check.handle("target", null, null, response);
    verifyFailed(response, stream);
  }
}
