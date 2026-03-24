package org.howietkl.kafka.request;

import org.howietkl.kafka.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class DescribeTopicPartitionsRequest extends Request {
  public static final Logger LOG = LoggerFactory.getLogger(DescribeTopicPartitionsRequest.class);
  public List<String> topicNames = new ArrayList<>();

  @Override
  public void parseRequest(ByteBuffer src) throws IOException {
    super.parseRequest(src);
    parseCompactArrayTopicNames(src);
  }

  void parseCompactArrayTopicNames(ByteBuffer src) throws IOException {
    int arraySize = Utils.getUnsignedVarInt(src) - 1;
    for (int i = 0; i < arraySize; i++) {
      topicNames.add(parseCompactStringTopicName(src));
    }
    topicNames.sort(Comparator.naturalOrder());
    LOG.debug("topics={}", topicNames.toString());
  }

  private String parseCompactStringTopicName(ByteBuffer src) throws IOException {
    int topicNameSize = Utils.getUnsignedVarInt(src) - 1;
    byte[] topicNameBytes = new byte[topicNameSize];
    src.get(topicNameBytes);
    String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
    src.get(); // tag buffer
    return topicName;
  }


}
