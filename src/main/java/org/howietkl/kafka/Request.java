package org.howietkl.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class Request {
  public static final short API_KEY_FETCH = 1;
  public static final short API_KEY_API_VERSIONS = 18;
  public static final short API_KEY_DESCRIBE_TOPIC_PARTITIONS = 75;

  public byte[] correlationId;
  public short requestApiKey;
  public short requestApiVersion;
  public String clientId;

  public static Request parseRequestHeader(ByteBuffer reqPayload) throws IOException {
    Request request;
    short requestApiKey = reqPayload.getShort();
    short requestApiVersion = reqPayload.getShort();
    byte[] correlationId = new byte[4];
    reqPayload.get(correlationId);
    String clientId = getClientId(reqPayload);
    reqPayload.get(); // tag buffer

    if (requestApiKey == API_KEY_API_VERSIONS) {
      if (requestApiVersion < 0 || requestApiVersion > 4) {
        request = new UnsupportedApiVersionErrorRequest();
      } else {
        request = new ApiVersionsRequest();
      }
    } else if (requestApiKey == API_KEY_DESCRIBE_TOPIC_PARTITIONS) {
      request = new DescribeTopicPartitionsRequest();
    } else if (requestApiKey == API_KEY_FETCH) {
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

  public void parseRequest(ByteBuffer reqPayload) throws IOException {
  }

  public static String getClientId(ByteBuffer reqPayload) {
    short clientIdSize = reqPayload.getShort();
    byte[] clientIdBytes = new byte[clientIdSize];
    reqPayload.get(clientIdBytes);
    return new String(clientIdBytes, StandardCharsets.UTF_8);
  }

}
