package io.fineo.read.serve.health;

import com.google.common.base.Joiner;
import io.fineo.read.serve.BaseInternalHandler;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.ByteArrayISO8859Writer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.UUID;

/**
 * Simple check to make sure the server is alive
 */
public class IsFineoAliveCheck extends BaseInternalHandler {
  private final JdbcMeta meta;

  public IsFineoAliveCheck(JdbcMeta meta) {
    super("GET", "alive", "fineo");
    this.meta = meta;
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request,
    HttpServletResponse response) throws IOException, ServletException {
    Meta.ConnectionHandle handle = new Meta.ConnectionHandle("health-check_" + UUID.randomUUID());
    try {
      meta.openConnection(handle, new HashMap<>());
      meta.getCatalogs(handle);
      response.setStatus(200);
      response.setContentLength(0);
    } catch (RuntimeException e) {
      response.setStatus(500);
      String message = e.getMessage();
      message += "\n" + Joiner.on(",\n").join(e.getStackTrace());
      response.setContentType("text/plain; charset=UTF-8");
      try (ByteArrayISO8859Writer writer = new ByteArrayISO8859Writer(1500);) {
        writer.write(message);
        writer.flush();
        response.setContentLength(writer.size());
        try (OutputStream out = response.getOutputStream()) {
          writer.writeTo(out);
        }
      }
    } finally {
      meta.closeConnection(handle);
    }
  }
}
