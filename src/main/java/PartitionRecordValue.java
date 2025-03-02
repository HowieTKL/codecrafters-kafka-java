public class PartitionRecordValue extends RecordValue {
  /*
- Value (Partition Record)
  - Partition ID
  - Topic UUID
  - Length of replica array
  - Replica Array
  - Length of In Sync Replica array
  - In Sync Replica Array
  - Length of Removing Replicas array
  - Length of Adding Replicas array
  - Leader
  - Leader Epoch
  - Partition Epoch
  - Length of Directories array
  - Directories Array
  - Tagged Fields Count
   */
  static final byte TYPE = 0x3; // 3

  @Override
  byte getType() {
    return TYPE;
  }

}
