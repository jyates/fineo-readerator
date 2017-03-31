package io.fineo.read.proxy.exception;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.jersey.errors.ErrorMessage;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import java.sql.SQLException;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Map an SqlException
 */
public class SqlExceptionMapper implements ExceptionMapper<SQLException> {
  private final Meter exceptions;

  public SqlExceptionMapper(MetricRegistry metrics) {
    exceptions = metrics.meter(name(getClass(), "sql-exceptions"));
  }

  @Override
  public Response toResponse(SQLException exception) {
    exceptions.mark();
    return Response.status(Response.Status.BAD_REQUEST)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(new ErrorMessage(Response.Status.BAD_REQUEST.getStatusCode(),
                     exception.getMessage()))
                   .build();
  }
}
