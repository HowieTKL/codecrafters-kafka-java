package org.howietkl.kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TopicPartitionFile {
  public static final String KAFKA_TOPIC_PARTITION_LOG_PATH = "/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log"; // %s= topic name, %d=partition index

  private Path path;
  public TopicPartitionFile(String topic, int partition) {
    path = Path.of(String.format(KAFKA_TOPIC_PARTITION_LOG_PATH, topic, partition));
  }
  public List<byte[]> getRecords() throws IOException {
    System.out.println("Reading: " + path);
    List<byte[]> records = new ArrayList<>();
    records.add(Files.readAllBytes(path));
    return records;
  }
}