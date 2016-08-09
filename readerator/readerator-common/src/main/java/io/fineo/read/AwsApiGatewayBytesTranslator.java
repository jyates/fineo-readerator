package io.fineo.read;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Base64;

/**
 * Translator that converts requests to a format that AWS API Gateway can support - specifically,
 * Base64 encoded Strings.
 */
public class AwsApiGatewayBytesTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(AwsApiGatewayBytesTranslator.class);
  private static final String DEFAULT_ENCODING = "UTF-8";
  public static final Charset UTF8 = Charset.forName(DEFAULT_ENCODING);
  private final Base64.Encoder encoder = Base64.getEncoder();
  private final Base64.Decoder decoder = Base64.getDecoder();

  public byte[] decode(String message) {
    LOG.warn("Decoding Encoded bytes:\n{}", message);
    return decoder.decode(message);
  }

  public byte[] decode(byte[] bytes) {
    return decode(new String(bytes, UTF8));
  }

  public byte[] encode(byte[] bytes) {
    String encoded = encoder.encodeToString(bytes);
    LOG.warn("Encoded bytes:\n{}", encoded);
    return encoded.getBytes(UTF8);
  }
}
