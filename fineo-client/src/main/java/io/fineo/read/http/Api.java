package io.fineo.read.http;

import io.fineo.read.http.model.Response;

/**
 *
 */
@com.amazonaws.mobileconnectors.apigateway.annotation.Service(endpoint = "SHOULD NEVER BE USED")
public
interface Api {

  @com.amazonaws.mobileconnectors.apigateway.annotation.Operation(path = "/send", method = "POST")
  Response send(byte[] data);
}
