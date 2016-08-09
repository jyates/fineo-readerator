package io.fineo.read.http.model;

public class Response {

  private final byte[] bytes;

  public Response(byte[] bytes) {
    this.bytes = bytes;
  }

  public byte[] getBytes() {
    return bytes;
  }
}
