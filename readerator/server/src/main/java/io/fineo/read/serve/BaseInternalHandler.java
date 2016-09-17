package io.fineo.read.serve;

import org.eclipse.jetty.server.Request;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.IOException;
import java.util.Collection;

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

  public boolean matches(Collection<Part> parts) {
    if (parts.size() != path.length) {
      return false;
    }

    int i = 0;
    for (Part p : parts) {
      String part = path[i++];
      if (!p.getName().equals(part)) {
        return false;
      }
    }
    return true;
  }

  public String getMethod() {
    return method;
  }
}
