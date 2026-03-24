package org.howietkl.kafka.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.howietkl.kafka.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Request {
  public static final short API_KEY_PRODUCE = 0;
  public static final short API_KEY_FETCH = 1;
  public static final short API_KEY_API_VERSIONS = 18;
  public static final short API_KEY_DESCRIBE_TOPIC_PARTITIONS = 75;

  public static final Logger LOG = LoggerFactory.getLogger(Request.class);

  public byte[] correlationId;
  public short requestApiKey;
  public short requestApiVersion;
  public String clientId;

  public static Request parseRequestHeader(ByteBuffer reqPayload) {
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
    } else if (requestApiKey == API_KEY_PRODUCE) {
      request = new ProduceRequest();
    } else {
      LOG.error("Unsupported request api key={}", requestApiKey);
      throw new UnsupportedOperationException("Unsupported request api key=" + requestApiKey);
    }
    request.requestApiKey = requestApiKey;
    request.requestApiVersion = requestApiVersion;
    request.clientId = clientId;
    request.correlationId = correlationId;
    LOG.debug("requestApiKey={}({}) requestApiVersion={} clientId={} correlationId={}",
        requestApiKey, request.getClass().getSimpleName(), requestApiVersion, clientId, Utils.bytesToHex(correlationId));
    return request;
  }

  public void parseRequest(ByteBuffer reqPayload) throws IOException {
    LOG.debug("Parsing request from {}", getClass().getSimpleName());
  }

  public static String getClientId(ByteBuffer reqPayload) {
    short clientIdSize = reqPayload.getShort();
    byte[] clientIdBytes = new byte[clientIdSize];
    reqPayload.get(clientIdBytes);
    return new String(clientIdBytes, StandardCharsets.UTF_8);
  }

}