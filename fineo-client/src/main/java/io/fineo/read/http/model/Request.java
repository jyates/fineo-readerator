package io.fineo.read.http.model;

public class Request {

  private final byte[] bytes;

  public Request(byte[] bytes) {
    this.bytes = bytes;
  }

  public byte[] getBytes() {
    return bytes;
  }
}
