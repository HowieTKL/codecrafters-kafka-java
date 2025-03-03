package org.howietkl.kafka.request;

import org.howietkl.kafka.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// https://kafka.apache.org/protocol.html#The_Messages_Fetch
public class FetchRequest extends Request {
  public List<byte[]> topicUUIDs = new ArrayList<>();
  public List<Integer> partitions = new ArrayList<>();

  @Override
  public void parseRequest(ByteBuffer reqPayload) throws IOException {
    /*
Fetch org.howietkl.kafka.request.Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] TAG_BUFFER
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic_id [partitions] TAG_BUFFER
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
     */
    System.out.println("parsing org.howietkl.kafka.request.FetchRequest");
    reqPayload.getInt(); // max_wait_ms
    reqPayload.getInt(); // min_bytes
    reqPayload.getInt(); // max_bytes
    reqPayload.get();    // isolation_level
    reqPayload.getInt(); // session_id
    reqPayload.getInt(); // session_epoch
    byte topicArrayLength = reqPayload.get();
    for (int i = 0; i < topicArrayLength - 1; i++) {
      byte[] topicUUID = new byte[16];
      reqPayload.get(topicUUID);
      topicUUIDs.add(topicUUID);
      System.out.println(" topicUUID=" + Utils.bytesToHex(topicUUID));
      byte partitionsArrayLength = reqPayload.get();
      for (int j = 0; j < partitionsArrayLength - 1; j++) {
        int partition = reqPayload.getInt();
        System.out.println(" partitionId=" + partition);
        partitions.add(partition); // partitionId
        reqPayload.getInt();  // current_leader_epoch
        reqPayload.getLong(); // fetch_offset
        reqPayload.getInt();  // last_fetched_epoch
        reqPayload.getLong();  // log_start_offset
        reqPayload.getInt();  // partition_max_bytes
      }
    }
  }
}
