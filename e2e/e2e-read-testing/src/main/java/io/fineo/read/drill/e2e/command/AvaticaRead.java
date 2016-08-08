package io.fineo.read.drill.e2e.command;

public class AvaticaRead extends Reader {

  public AvaticaRead() {
    super("org.apache.calcite.avatica.remote.Driver");
  }

  @Override
  public String getJdbcConnection(String url) {
    return "jdbc:avatica:remote:serialization=protobuf;"+url;
  }
}
