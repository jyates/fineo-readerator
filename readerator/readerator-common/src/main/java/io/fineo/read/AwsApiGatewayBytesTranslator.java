package io.fineo.read;

import java.nio.charset.Charset;
import java.util.Base64;

/**
 * Translator that converts requests to a format that AWS API Gateway can support - specifically,
 * Base64 encoded Strings.
 */
public class AwsApiGatewayBytesTranslator {

  private static final String DEFAULT_ENCODING = "UTF-8";
  public static final Charset UTF8 = Charset.forName(DEFAULT_ENCODING);
  private final Base64.Encoder encoder = Base64.getEncoder();
  private final Base64.Decoder decoder = Base64.getDecoder();

  public byte[] decode(byte[] bytes){
    return decoder.decode(new String(bytes, UTF8));
  }

  public byte[] encode(byte[] bytes){
    return encoder.encodeToString(bytes).getBytes(UTF8);
  }
}
