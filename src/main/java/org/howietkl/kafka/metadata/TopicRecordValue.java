package org.howietkl.kafka.metadata;

public class TopicRecordValue extends RecordValue {
/*
↓ Value (Topic Record)
- Frame Version
- Type
- Version
- Name length
- Topic Name
- Topic UUID
- Tagged Fields Count
 */
  static final byte TYPE = 0x2; // 2
  byte version;
  String topicName;
  byte[] topicUUID = new byte[16];
  int taggedFieldsCount; // varint unsigned

  @Override
  byte getType() {
    return TYPE;
  }
}
