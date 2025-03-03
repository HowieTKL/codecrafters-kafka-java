package org.howietkl.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;

// https://binspec.org/kafka-cluster-metadata
public class Metadata {
  public static final String KAFKA_CLUSTER_METADATA_LOG_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
  public static final Logger LOG = LoggerFactory.getLogger(Metadata.class);

  private static final Metadata instance = new Metadata();
  private static final List<RecordBatch> recordBatches = new ArrayList<>();
  private static final Map<UUID, String> topicUUID2Name = new HashMap<>();

  static {
    try {
      readMetadataLog();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private Metadata() {
  }

  public static Metadata getInstance() {
    return instance;
  }

  public byte[] findTopicUUID(String topicName) {
    for (RecordBatch recordBatch : recordBatches) {
      for (Record record : recordBatch.records) {
        if (record.recordValue.getType() == TopicRecordValue.TYPE) {
          TopicRecordValue topicRecordValue = (TopicRecordValue) record.recordValue;
          if (topicRecordValue.topicName.equals(topicName)) {
            return topicRecordValue.topicUUID;
          }
        }
      }
    }
    return null;
  }

  public List<PartitionRecordValue> findPartitionRecordValues(byte[] topicUUID) {
    List<PartitionRecordValue> partitionRecordValues = new ArrayList<>();
    for (RecordBatch recordBatch : recordBatches) {
      for (Record record : recordBatch.records) {
        if (record.recordValue.getType() == PartitionRecordValue.TYPE) {
          PartitionRecordValue partitionRecordValue = (PartitionRecordValue) record.recordValue;
          if (Arrays.equals(partitionRecordValue.topicUUID, topicUUID)) {
            partitionRecordValues.add(partitionRecordValue);
          }
        }
      }
    }
    return partitionRecordValues;
  }

  public String findTopicName(byte[] topicUUID) {
    return topicUUID2Name.get(UUID.nameUUIDFromBytes(topicUUID));
  }

  // FYI not thread safe, but fine for this single thread reading from file
  private static void readMetadataLog() throws IOException {
    LOG.info("Reading={}", KAFKA_CLUSTER_METADATA_LOG_PATH);
    ByteBuffer src;
    try (InputStream is = new FileInputStream(KAFKA_CLUSTER_METADATA_LOG_PATH)) {
      src = ByteBuffer.wrap(is.readAllBytes());
    }
    while (src.hasRemaining()) {
      recordBatches.add(getRecordBatch(src));
    }
  }

  private static RecordBatch getRecordBatch(ByteBuffer src) throws IOException {
    LOG.debug("Parsing a record batch");
    RecordBatch recBatch = new RecordBatch();
    src.get(recBatch.baseOffset);
    src.get(recBatch.batchLength);
    src.get(recBatch.partitionLeaderEpoch);
    recBatch.magicByte = src.get();
    src.get(recBatch.crc);
    src.get(recBatch.attributes);
    src.get(recBatch.lastOffsetDelta);
    src.get(recBatch.baseTimestamp);
    src.get(recBatch.maxTimestamp);
    src.get(recBatch.producerId);
    src.get(recBatch.producerEpoch);
    src.get(recBatch.baseSequence);
    recBatch.records = getRecords(src);
    return recBatch;
  }

  private static List<Record> getRecords(ByteBuffer src) throws IOException {
    List<Record> records = new ArrayList<>();
    int numRecords = src.getInt();
    for (int i = 0; i < numRecords; i++) {
      records.add(getRecord(src));
    }
    return records;
  }

  private static Record getRecord(ByteBuffer src) throws IOException {
    LOG.debug("Parsing a record");
    Record record = new Record();
    record.length = Utils.getSignedVarInt(src);
    record.attributes = src.get(); // attributes (unused)
    record.timestamp = Utils.getSignedVarInt(src);
    record.offset = Utils.getSignedVarInt(src);
    record.keyLength = Utils.getSignedVarInt(src);
    if (record.keyLength > 0) {
      record.key = new byte[record.keyLength];
      src.get(record.key);
    }
    record.valueLength = Utils.getSignedVarInt(src);
    LOG.debug("record value length={}", record.valueLength);
    if (record.valueLength > 0) {
      byte[] recordValueBytes = new byte[record.valueLength];
      src.get(recordValueBytes);
      ByteBuffer rvSrc = ByteBuffer.wrap(recordValueBytes);
      byte frameVersion = rvSrc.get();
      int type = rvSrc.get();
      LOG.debug("record value type={}", type);
      switch (type) {
        case TopicRecordValue.TYPE -> record.recordValue = getTopicRecordValue(rvSrc);
        case PartitionRecordValue.TYPE -> record.recordValue = getPartitionRecordValue(rvSrc);
        case FeatureLevelRecordValue.TYPE -> record.recordValue = getFeatureLevelRecordValue(rvSrc);
        default -> LOG.warn("Ignoring unsupported record value type={}", type);
      }
      if (record.recordValue != null) {
        record.recordValue.frameVersion = frameVersion;
      }
      record.headersArrayCount = Utils.getUnsignedVarInt(src);
    }
    return record;
  }

  private static TopicRecordValue getTopicRecordValue(ByteBuffer src) throws IOException {
    TopicRecordValue recordValue = new TopicRecordValue();
    recordValue.version = src.get();
    recordValue.topicName = Utils.getCompactString(src);
    src.get(recordValue.topicUUID);
    LOG.debug("topicName={} topicUUID={}", recordValue.topicName, Utils.bytesToHex(recordValue.topicUUID));
    recordValue.taggedFieldsCount = Utils.getUnsignedVarInt(src);

    // update index
    UUID uuid = UUID.nameUUIDFromBytes(recordValue.topicUUID);
    topicUUID2Name.put(UUID.nameUUIDFromBytes(recordValue.topicUUID), recordValue.topicName);

    return recordValue;
  }

  private static PartitionRecordValue getPartitionRecordValue(ByteBuffer src) throws IOException {
    PartitionRecordValue recordValue = new PartitionRecordValue();
    recordValue.version = src.get();
    src.get(recordValue.partitionId);
    src.get(recordValue.topicUUID);
    LOG.debug("partitionId={} topicUUID={}",
        ByteBuffer.wrap(recordValue.partitionId).getInt(), Utils.bytesToHex(recordValue.topicUUID));

    {
      int replicaArrayLength = Utils.getUnsignedVarInt(src) - 1;
      for (int i = 0; i < replicaArrayLength; i++) {
        byte[] replicaArray = new byte[4];
        src.get(replicaArray);
        recordValue.replicaArray.add(replicaArray);
      }
    }
    {
      int inSyncReplicaArrayLength = Utils.getUnsignedVarInt(src) - 1;
      for (int i = 0; i < inSyncReplicaArrayLength; i++) {
        byte[] inSyncReplicaArray = new byte[4];
        src.get(inSyncReplicaArray);
        recordValue.inSyncReplicaArray.add(inSyncReplicaArray);
      }
    }
    {
      int removingReplicasArrayLength = Utils.getUnsignedVarInt(src) - 1;
      for (int i = 0; i < removingReplicasArrayLength; i++) {
        byte[] removingReplicasArray = new byte[4];
        src.get(removingReplicasArray);
        recordValue.removingReplicasArray.add(removingReplicasArray);
      }
    }
    {
      int addingReplicasArrayLength = Utils.getUnsignedVarInt(src) - 1;
      for (int i = 0; i < addingReplicasArrayLength; i++) {
        byte[] addingReplicasArray = new byte[4];
        src.get(addingReplicasArray);
        recordValue.addingReplicasArray.add(addingReplicasArray);
      }
    }
    src.get(recordValue.leader);
    src.get(recordValue.leaderEpoch);
    src.get(recordValue.partitionEpoch);
    {
      int directoriesArrayLength = Utils.getUnsignedVarInt(src) - 1;
      for (int i = 0; i < directoriesArrayLength; i++) {
        byte[] directoriesArray = new byte[16];
        src.get(directoriesArray);
        recordValue.directoriesArray.add(directoriesArray);
      }
    }
    recordValue.taggedFieldsCount = Utils.getUnsignedVarInt(src);
    return recordValue;
  }

  private static FeatureLevelRecordValue getFeatureLevelRecordValue(ByteBuffer src) throws IOException {
    return new FeatureLevelRecordValue();
  }

}