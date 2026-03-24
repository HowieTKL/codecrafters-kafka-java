package org.howietkl.kafka.metadata;

import java.util.List;

public class RecordBatch {
  byte[] baseOffset = new byte[8]; // 8 bytes
  byte[] batchLength = new byte[4]; // 4
  byte[] partitionLeaderEpoch = new byte[4]; // 4
  byte magicByte; // 1
  byte[] crc = new byte[4]; // 4
  byte[] attributes = new byte[2]; // 2
  byte[] lastOffsetDelta = new byte[4]; // 4
  byte[] baseTimestamp = new byte[8]; // 8
  byte[] maxTimestamp = new byte[8]; // 8
  byte[] producerId = new byte[8]; // 8
  byte[] producerEpoch = new byte[2]; // 2
  byte[] baseSequence = new byte[4]; // 4
  List<Record> records;
}