import java.util.ArrayList;
import java.util.List;

class RecordBatch {
  byte[] baseOffset; // 8 bytes
  byte[] batchLength; // 4
  byte[] partitionLeaderEpoch; // 4
  byte[] magicByte; // 1
  byte[] crc; // 4
  byte[] attributes; // 2
  byte[] lastOffsetDelta; // 4
  byte[] baseTimestamp; // 8
  byte[] maxTimestamp; // 8
  byte[] producerId; // 8
  byte[] producerEpoch; // 2
  byte[] baseSequence; // 4
  List<Record> records;
}