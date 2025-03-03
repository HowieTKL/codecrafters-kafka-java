import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;

class Metadata {
  static final Metadata instance = new Metadata();

  private List<RecordBatch> recordBatches;
  private final Map<UUID, String> topicUUID2Name = new HashMap<>();

  private Metadata() {
    try {
      recordBatches = getMetadataLog();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static Metadata getInstance() {
    return instance;
  }

  byte[] findTopicUUID(String topicName) {
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

  List<PartitionRecordValue> findPartitionRecordValues(byte[] topicUUID) {
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

  String findTopicName(byte[] topicUUID) {
    return topicUUID2Name.get(UUID.nameUUIDFromBytes(topicUUID));
  }

  // FYI not thread safe, but fine for this single thread reading from file
  static List<RecordBatch> getMetadataLog() throws IOException {
    System.out.println("Attempting to read: " + Main.KAFKA_CLUSTER_METADATA_LOG_PATH);
    List<RecordBatch> recordBatches = new ArrayList<>();
    ByteBuffer src;
    try (InputStream is = new FileInputStream(Main.KAFKA_CLUSTER_METADATA_LOG_PATH)) {
      src = ByteBuffer.wrap(is.readAllBytes());
    }
    while (src.hasRemaining()) {
      recordBatches.add(getRecordBatch(src));
    }
    return recordBatches;
  }

  static RecordBatch getRecordBatch(ByteBuffer src) throws IOException {
    System.out.println("Parsing record batch");
    RecordBatch recBatch = new RecordBatch();
    src.get(recBatch.baseOffset);
    System.out.println("Base offset: " + ByteBuffer.wrap(recBatch.baseOffset).getLong());
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

  static List<Record> getRecords(ByteBuffer src) throws IOException {
    System.out.println("Parsing records");
    List<Record> records = new ArrayList<>();
    int numRecords = src.getInt();
    for (int i = 0; i < numRecords; i++) {
      records.add(getRecord(src));
    }
    return records;
  }

  static Record getRecord(ByteBuffer src) throws IOException {
    System.out.println("Parsing record");
    Record record = new Record();
    record.length = Utils.getSignedVarInt(src);
    System.out.println("record length: " + record.length);
    record.attributes = src.get(); // attributes (unused)
    record.timestamp = Utils.getSignedVarInt(src);
    record.offset = Utils.getSignedVarInt(src);
    record.keyLength = Utils.getSignedVarInt(src);
    if (record.keyLength > 0) {
      record.key = new byte[record.keyLength];
      src.get(record.key);
    }
    record.valueLength = Utils.getSignedVarInt(src);
    System.out.println("record value length: " + record.valueLength);
    if (record.valueLength > 0) {
      byte[] recordValueBytes = new byte[record.valueLength];
      src.get(recordValueBytes);
      ByteBuffer rvSrc = ByteBuffer.wrap(recordValueBytes);
      byte frameVersion = rvSrc.get();
      int type = rvSrc.get();
System.out.println("record value type: " + type);
      switch (type) {
        case TopicRecordValue.TYPE -> record.recordValue = getTopicRecordValue(rvSrc);
        case PartitionRecordValue.TYPE -> record.recordValue = getPartitionRecordValue(rvSrc);
        case FeatureLevelRecordValue.TYPE -> record.recordValue = getFeatureLevelRecordValue(rvSrc);
        default -> System.out.println("Unexpected record value type: " + type);
      }
      if (record.recordValue != null) {
        record.recordValue.frameVersion = frameVersion;
      }
      record.headersArrayCount = Utils.getUnsignedVarInt(src);
    }
    return record;
  }

  static TopicRecordValue getTopicRecordValue(ByteBuffer src) throws IOException {
    System.out.println("   TopicRecordValue");
    TopicRecordValue recordValue = new TopicRecordValue();
    recordValue.version = src.get();
    recordValue.topicName = Utils.getCompactString(src);
    System.out.println("    topicName:" + recordValue.topicName);
    src.get(recordValue.topicUUID);
    System.out.println("    topicUUID:" + Utils.bytesToHex(recordValue.topicUUID));
    recordValue.taggedFieldsCount = Utils.getUnsignedVarInt(src);

    // update index
    UUID uuid = UUID.nameUUIDFromBytes(recordValue.topicUUID);
    getInstance().topicUUID2Name.put(UUID.nameUUIDFromBytes(recordValue.topicUUID), recordValue.topicName);

    return recordValue;
  }

  static PartitionRecordValue getPartitionRecordValue(ByteBuffer src) throws IOException {
    System.out.println("   PartitionRecordValue");
    PartitionRecordValue recordValue = new PartitionRecordValue();
    recordValue.version = src.get();
    src.get(recordValue.partitionId);
    System.out.println("    partitionId:" + Utils.bytesToHex(recordValue.partitionId));
    src.get(recordValue.topicUUID);
    System.out.println("    topicUUID:" + Utils.bytesToHex(recordValue.topicUUID));

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

  static FeatureLevelRecordValue getFeatureLevelRecordValue(ByteBuffer src) throws IOException {
    System.out.println("   FeatureLevelRecordValue");
    return new FeatureLevelRecordValue();
    // todo
  }

}
