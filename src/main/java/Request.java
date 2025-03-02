import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class Request {
  byte[] correlationId;
  short requestApiKey;
  short requestApiVersion;
  String clientId;

  static Request parseRequestHeader(ByteBuffer reqPayload) throws IOException {
    Request request;
    short requestApiKey = reqPayload.getShort();
    short requestApiVersion = reqPayload.getShort();
    byte[] correlationId = new byte[4];
    reqPayload.get(correlationId);
    String clientId = getClientId(reqPayload);
    reqPayload.get(); // tag buffer

    if (requestApiVersion < 0 || requestApiVersion > 4) {
      request = new UnsupportedApiVersionErrorRequest();
    } else if (requestApiKey == Main.API_KEY_API_VERSIONS) {
      request = new ApiVersionsRequest();
    } else if (requestApiKey == Main.API_KEY_DESCRIBE_TOPIC_PARTITIONS) {
      request = new DescribeTopicPartitionsRequest();
    } else if (requestApiKey == Main.API_KEY_FETCH) {
      request = new FetchRequest();
    } else {
      throw new IllegalArgumentException("Unknown request api key");
    }
    request.requestApiKey = requestApiKey;
    request.requestApiVersion = requestApiVersion;
    request.clientId = clientId;
    request.correlationId = correlationId;

    System.out.println("request clientId=" + clientId);
    System.out.println("request correlationId=" + Utils.bytesToHex(correlationId));
    return request;
  }

  void parseRequest(ByteBuffer reqPayload) throws IOException {
  }

  static String getClientId(ByteBuffer reqPayload) {
    short clientIdSize = reqPayload.getShort();
    byte[] clientIdBytes = new byte[clientIdSize];
    reqPayload.get(clientIdBytes);
    return new String(clientIdBytes, StandardCharsets.UTF_8);
  }

}
