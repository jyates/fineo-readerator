package io.fineo.read.proxy.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import static java.lang.String.format;

public class MissingParameterWebException extends WebApplicationException {

  public MissingParameterWebException(String paramName, String message){
    super(format("Invalid parameter: %s. %s", paramName, message), Response.Status.BAD_REQUEST);
  }
}
