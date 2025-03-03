package org.howietkl.kafka;

public class FeatureLevelRecordValue extends RecordValue {
  /*
- Value (Feature Level org.howietkl.kafka.Record)
  - Name length
  - Name
  - Feature Level
  - Tagged Fields Count
   */
  static final byte TYPE = 0x0c; // 12

  @Override
  byte getType() {
    return TYPE;
  }
}
