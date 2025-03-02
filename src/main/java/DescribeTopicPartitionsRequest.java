import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DescribeTopicPartitionsRequest extends Request {
  List<String> topicNames = new ArrayList<>();

  @Override
  void parseRequest(ByteBuffer src) throws IOException {
    parseCompactArrayTopicNames(src);
  }

  void parseCompactArrayTopicNames(ByteBuffer src) throws IOException {
    int arraySize = Utils.getUnsignedVarInt(src) - 1;
    System.out.println(" numTopics=" + arraySize);
    for (int i = 0; i < arraySize; i++) {
      topicNames.add(parseCompactStringTopicName(src));
    }
  }

  private String parseCompactStringTopicName(ByteBuffer src) throws IOException {
    int topicNameSize = Utils.getUnsignedVarInt(src) - 1;
    byte[] topicNameBytes = new byte[topicNameSize];
    src.get(topicNameBytes);
    String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
    System.out.println(" request topic=" + topicName);
    src.get(); // tag buffer
    return topicName;
  }


}
