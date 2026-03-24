package org.howietkl.kafka.response;

import org.howietkl.kafka.TopicPartitionFile;
import org.howietkl.kafka.Utils;
import org.howietkl.kafka.metadata.Metadata;
import org.howietkl.kafka.request.FetchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class FetchResponse extends Response<FetchRequest> {
  public static final Logger LOG = LoggerFactory.getLogger(FetchResponse.class);

  @Override
  public void processResponse(FetchRequest request, OutputStream out) throws IOException {
    LOG.info("processResponse API_KEY_FETCH");
    out.write(0); // tag buffer
    out.write(new byte[]{0, 0, 0, 0}); // throttle time
    out.write(ERR_NONE);
    out.write(new byte[]{0, 0, 0, 0}); // session id
    Utils.putUnsignedVarInt(out, request.topicUUIDs.size() + 1);
    for (int i = 0; i < request.topicUUIDs.size(); i++) {
      byte[] topicUUID = request.topicUUIDs.get(i);
      out.write(topicUUID);
      List<Integer> partitions = request.partitions;
      if (partitions.isEmpty()) {
        out.write((byte) 2); // partitions=1
        out.write(new byte[]{0, 0, 0, 0}); // partition index
        out.write(hasTopic(topicUUID) ? ERR_NONE : ERR_UNKNOWN_TOPIC);
        out.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // high watermark
        out.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // last_stable_offset
        out.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // log_start_offset
        Utils.putUnsignedVarInt(out, 0); // varint aborted_transactions
        // resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // aborted producer id
        // resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // aborted first offset
        out.write(new byte[]{0, 0, 0, 0}); // preferred read replica
        Utils.putUnsignedVarInt(out, 0); // varint records
        out.write((byte) 0); // tag buffer - partition
      } else {
        Utils.putUnsignedVarInt(out, partitions.size() + 1);
        for (Integer partitionId : partitions) {
          out.write(ByteBuffer.allocate(4).putInt(partitionId).array());
          out.write(hasTopic(topicUUID) ? ERR_NONE : ERR_UNKNOWN_TOPIC);
          out.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // high watermark
          out.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // last_stable_offset
          out.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // log_start_offset
          Utils.putUnsignedVarInt(out, 0); // varint aborted_transactions
          // resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // aborted producer id
          // resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // aborted first offset
          out.write(new byte[]{0, 0, 0, 0}); // preferred read replica
          List<byte[]> records = getRecords(topicUUID, partitionId);
          Utils.putUnsignedVarInt(out, records.isEmpty() ? 0 : records.getFirst().length + 1);
          for (byte[] record : records) {
            out.write(record);
          }
          out.write((byte) 0); // tag buffer - partition
        }
      }
      out.write((byte) 0); // tag buffer - topic
    }
    out.write((byte) 0); // tag buffer
  }

  private boolean hasTopic(byte[] topicUUID) {
    return Metadata.getInstance().findTopicName(topicUUID) != null;
  }

  private static List<byte[]> getRecords(byte[] topicUUID, int partitionId) throws IOException {
    LOG.info("getRecords topicUUID={} partitionId={}", Utils.bytesToHex(topicUUID), partitionId);
    String topicName = Metadata.getInstance().findTopicName(topicUUID);
    if (topicName != null) {
      TopicPartitionFile file = new TopicPartitionFile(topicName, partitionId);
      return file.getRecords();
    } else {
      return Collections.emptyList();
    }
  }

}
