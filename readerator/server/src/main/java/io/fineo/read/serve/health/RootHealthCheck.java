package io.fineo.read.serve.health;

import io.fineo.read.serve.BaseInternalHandler;
import org.eclipse.jetty.server.Request;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Simple check to make sure the server is alive at the "/". Needed b/c EB keeps trying to  GET "/"
 */
public class RootHealthCheck extends BaseInternalHandler {
  public RootHealthCheck() {
    super("GET");
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request,
    HttpServletResponse response) throws IOException, ServletException {
    response.setStatus(200);
    response.setContentLength(0);
  }
}
