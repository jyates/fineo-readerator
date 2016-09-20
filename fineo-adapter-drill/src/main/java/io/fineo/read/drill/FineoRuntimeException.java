package io.fineo.read.drill;

/**
 * Exception to throw when there is an issue and we want to have people send the error.
 */
public class FineoRuntimeException extends RuntimeException {
  public FineoRuntimeException(String message) {
    super(message + "\n  --- Please send this message/stack trace to: help@fineo.io ----");
  }
}
