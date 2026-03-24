package org.howietkl.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TopicPartitionFile {
  private static final Logger LOG = LoggerFactory.getLogger(TopicPartitionFile.class);
  public static final String KAFKA_TOPIC_PARTITION_LOG_PATH = "/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log"; // %s= topic name, %d=partition index

  private final Path path;
  public TopicPartitionFile(String topic, int partition) {
    path = Path.of(String.format(KAFKA_TOPIC_PARTITION_LOG_PATH, topic, partition));
  }
  public List<byte[]> getRecords() throws IOException {
    LOG.debug("Reading={}", path);
    byte[] data = Files.readAllBytes(path);
    LOG.debug("Contents={}", Utils.bytesToHex(data));
    List<byte[]> records = new ArrayList<>();
    if (data.length > 0) {
      records.add(data);
    }
    return records;
  }
}