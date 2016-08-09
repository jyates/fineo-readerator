package io.fineo.read.serve;

import io.fineo.read.AwsApiGatewayBytesTranslator;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.Service;

import java.io.IOException;

/**
 * AWS API Gateway only support json strings, so we have to Base64 encode requests as strings and
 * then send them along. On the receive side (here) we do the opposite and decode the Base64
 * encoded strings into raw bytes
 */
public class AwsStringBytesDecodingTranslator extends ProtobufTranslationImpl {

  private final AwsApiGatewayBytesTranslator translator = new AwsApiGatewayBytesTranslator();

  @Override
  public Service.Request parseRequest(byte[] bytes) throws IOException {
    return super.parseRequest(translator.decode(bytes));
  }

  @Override
  public byte[] serializeResponse(Service.Response response) throws IOException {
    return translator.encode(super.serializeResponse(response));
  }
}
