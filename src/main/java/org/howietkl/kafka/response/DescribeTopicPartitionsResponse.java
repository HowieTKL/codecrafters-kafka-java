package org.howietkl.kafka.response;

import org.howietkl.kafka.Utils;
import org.howietkl.kafka.metadata.Metadata;
import org.howietkl.kafka.metadata.PartitionRecordValue;
import org.howietkl.kafka.request.DescribeTopicPartitionsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class DescribeTopicPartitionsResponse extends Response<DescribeTopicPartitionsRequest> {
  public static final Logger LOG = LoggerFactory.getLogger(DescribeTopicPartitionsResponse.class);

  @Override
  public void processResponse(DescribeTopicPartitionsRequest request, OutputStream out) throws IOException {
    LOG.info("processResponse API_KEY_DESCRIBE_TOPIC_PARTITIONS");
    out.write(new byte[]{0}); // tag buffer
    out.write(new byte[]{0,0,0,0}); // throttle time

    // topics
    Utils.putUnsignedVarInt(out, request.topicNames.size() + 1); // compact array size
    // for each topic
    for (int i = 0; i < request.topicNames.size(); i++) {
      String topicName = request.topicNames.get(i);
      byte[] topicUUID = Metadata.getInstance().findTopicUUID(topicName);
      if (topicUUID != null) {
        out.write(ERR_NONE);
        Utils.putCompactString(out, topicName);
        out.write(topicUUID); // topic id
        out.write(new byte[]{0}); // is internal
        List<PartitionRecordValue> partitions = Metadata.getInstance().findPartitionRecordValues(topicUUID);
        Utils.putUnsignedVarInt(out, partitions.size() + 1);
        for (PartitionRecordValue partition : partitions) {
          out.write(ERR_NONE);
          out.write(partition.getPartitionId());
          out.write(partition.getLeader());
          out.write(partition.getLeaderEpoch());
          Utils.putUnsignedVarInt(out, partition.getReplicaArray().size() + 1);
          for (byte[] replica: partition.getReplicaArray()) {
            out.write(replica);
          }
          Utils.putUnsignedVarInt(out, partition.getInSyncReplicaArray().size() + 1);
          for (byte[] inSyncReplica: partition.getInSyncReplicaArray()) {
            out.write(inSyncReplica);
          }
          out.write(new byte[]{1}); // eligible leader replicas
          out.write(new byte[]{1}); // last known eligible leader replicas
          out.write(new byte[]{1}); // last known offline replicas
          out.write(new byte[]{0}); // tag buffer
        }
        out.write(new byte[]{0, 0, 0xD, (byte) 0xF8}); // topic authorized operations
        out.write(new byte[]{0}); // tag buffer
      } else {
        out.write(ERR_UNKNOWN_TOPIC_OR_PARTITION);
        Utils.putCompactString(out, request.topicNames.get(i));
        out.write(new byte[16]); // topic id
        out.write(new byte[]{0}); // is internal
        out.write(new byte[]{1}); // partitions array
        out.write(new byte[]{0, 0, 0xD, (byte) 0xF8}); // topic authorized operations
        out.write(new byte[]{0}); // tag buffer
      }
    }
    out.write(new byte[]{(byte) 0xFF}); // cursor
    out.write(new byte[]{0}); // tag buffer
  }
}
