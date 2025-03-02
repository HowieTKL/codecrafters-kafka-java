import java.util.ArrayList;
import java.util.List;

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
  byte[] partitionId = new byte[4];
  byte[] topicUUID = new byte[16];
  List<byte[]> replicaArray = new ArrayList<>(); // 4-bytes each
  List<byte[]> inSyncReplicaArray = new ArrayList<>(); // 4-bytes each
  List<byte[]> removingReplicasArray = new ArrayList<>(); // 4-bytes each
  List<byte[]> addingReplicasArray = new ArrayList<>(); // 4-bytes each
  byte[] leader = new byte[4]; // 4-bytes
  byte[] leaderEpoch = new byte[4]; // 4-bytes
  byte[] partitionEpoch = new byte[4]; // 4-bytes
  List<byte[]> directoriesArray = new ArrayList<>(); // 16-bytes directory UUIDs
  int taggedFieldsCount; // varint unsigned

  @Override
  byte getType() {
    return TYPE;
  }

}
