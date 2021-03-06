package io.fineo.read.serve;

import org.eclipse.jetty.server.Request;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import java.io.IOException;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBaseInternalHandler {

  @Test
  public void testPathMatches() throws Exception {
    BaseInternalHandler handler = new BaseInternalHandler("a", "a", "b", "c") {
      @Override
      public void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException, ServletException {
      }
    };
    assertTrue(handler.matches(new String[]{"a", "b", "c"}));
    assertFalse(handler.matches(new String[]{"a", "b"}));
    assertFalse(handler.matches(new String[]{}));
    assertFalse(handler.matches(null));
    assertFalse(handler.matches(new String[]{"a", "b", "c", "c"}));
  }

  @Test
  public void testNoPath() throws Exception {
    BaseInternalHandler handler = new BaseInternalHandler("a") {
      @Override
      public void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException, ServletException {
      }
    };
    assertTrue(handler.matches(new String[]{}));
    assertTrue(handler.matches(null));
    assertFalse(handler.matches(new String[]{"a"}));
  }
}
