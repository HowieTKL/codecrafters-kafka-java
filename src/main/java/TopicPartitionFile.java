import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class TopicPartitionFile {
  private Path path;
  public TopicPartitionFile(String topic, int partition) {
    path = Path.of(String.format(Main.KAFKA_TOPIC_PARTITION_LOG_PATH, topic, partition));
  }
  public byte[] getData() throws IOException {
    System.out.println("Reading: " + path);
    return Files.readAllBytes(path);
  }
}