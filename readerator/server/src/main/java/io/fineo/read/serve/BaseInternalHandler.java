package io.fineo.read.serve;

import org.eclipse.jetty.server.Request;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.IOException;
import java.util.Arrays;

/**
 * Simple handler that specifies the method and path that it handles. Similar to something like
 * JaxRS, but without all the reflection. For right now, this is easier to use, but maybe later
 * we should move to something more flexible.
 */
public abstract class BaseInternalHandler {

  private final String method;
  private final String[] path;

  public BaseInternalHandler(String method, String... path) {
    this.method = method;
    this.path = path;
  }
  public abstract void handle(String target, Request baseRequest,
    HttpServletRequest request, HttpServletResponse response) throws
    IOException, ServletException;

  public boolean matches(String[] parts) {
    boolean empty = parts == null || parts.length == 0;
    if(empty && (path == null || path.length == 0)){
      return true;
    }

    return Arrays.equals(path, parts);
  }

  public String getMethod() {
    return method;
  }
}
