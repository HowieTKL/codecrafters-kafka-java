import org.howietkl.kafka.TopicPartitionFile;
import org.howietkl.kafka.Utils;
import org.howietkl.kafka.metadata.Metadata;
import org.howietkl.kafka.metadata.PartitionRecordValue;
import org.howietkl.kafka.request.*;
import org.howietkl.kafka.response.UnsupportedApiVersionErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  private static final int PORT = 9092;
  private static final int THREADS = 4;

  public static final byte[] ERR_UNSUPPORTED_VERSION = new byte[]{0, 35};
  public static final byte[] ERR_NONE = new byte[]{0, 0};
  public static final byte[] ERR_UNKNOWN_TOPIC_OR_PARTITION = new byte[]{0, 3};
  public static final byte[] ERR_UNKNOWN_TOPIC = new byte[]{0, 100};

  public static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    try (ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
         ServerSocket serverSocket = new ServerSocket(PORT)) {
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      while (true) {
        LOG.info("Server port={} waiting for connection...", PORT);
        // Wait for connection from client.
        Socket clientSocket = serverSocket.accept();
        executorService.submit(() -> handleRequest(clientSocket));
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  static void handleRequest(Socket clientSocket) {
    try (clientSocket) {
      InputStream in = clientSocket.getInputStream();
      while (true) {
        byte[] messageSizeBytes = new byte[4];
        if (in.read(messageSizeBytes) <= 0) {
          break;
        }
        int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();
        LOG.info("Message size={} bytes", messageSize);
        ByteBuffer reqPayload = ByteBuffer.wrap(in.readNBytes(messageSize));
        ByteArrayOutputStream resPayload = new ByteArrayOutputStream();
        handleRequest(reqPayload, resPayload);
        OutputStream out = clientSocket.getOutputStream();
        out.write(ByteBuffer.allocate(4).putInt(resPayload.size()).array()); // message/payload size
        out.write(resPayload.toByteArray());
        out.flush();
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  static void handleRequest(ByteBuffer reqPayload, OutputStream resPayload) throws IOException {
    Request request = Request.parseRequestHeader(reqPayload);
    request.parseRequest(reqPayload);
    resPayload.write(request.correlationId);
    switch (request) {
      case UnsupportedApiVersionErrorRequest unsupportedApiVersionErrorRequest ->
        new UnsupportedApiVersionErrorResponse().processResponse(unsupportedApiVersionErrorRequest, resPayload);
      case ApiVersionsRequest apiVersionsRequest ->
          handleApiVersions(apiVersionsRequest, resPayload);
      case DescribeTopicPartitionsRequest describeTopicPartitionsRequest ->
          handleDescribeTopicPartitions(describeTopicPartitionsRequest, resPayload);
      case FetchRequest fetchRequest ->
          handleFetch(fetchRequest, resPayload);
      default -> {
        LOG.error("Unsupported request type={}", request.getClass().getName());
        throw new UnsupportedOperationException("Unknown request type: " + request);
      }
    }
  }

  static void handleFetch(FetchRequest request, OutputStream resPayload) throws IOException {
    LOG.debug("handleRequest API_KEY_FETCH");
    resPayload.write(0); // tag buffer
    resPayload.write(new byte[]{0, 0, 0, 0}); // throttle time
    resPayload.write(ERR_NONE);
    resPayload.write(new byte[]{0, 0, 0, 0}); // session id
    Utils.putUnsignedVarInt(resPayload, request.topicUUIDs.size() + 1);
    for (int i = 0; i < request.topicUUIDs.size(); i++) {
      byte[] topicUUID = request.topicUUIDs.get(i);
      resPayload.write(topicUUID);
      List<Integer> partitions = request.partitions;
      if (partitions.isEmpty()) {
        resPayload.write((byte) 2); // partitions=1
        resPayload.write(new byte[]{0, 0, 0, 0}); // partition index
        resPayload.write(ERR_UNKNOWN_TOPIC);
        resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // high watermark
        resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // last_stable_offset
        resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // log_start_offset
        Utils.putUnsignedVarInt(resPayload, 0); // varint aborted_transactions
        // resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // aborted producer id
        // resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // aborted first offset
        resPayload.write(new byte[]{0, 0, 0, 0}); // preferred read replica
        Utils.putUnsignedVarInt(resPayload, 0); // varint records
        resPayload.write((byte) 0); // tag buffer - partition
      } else {
        Utils.putUnsignedVarInt(resPayload, partitions.size() + 1);
        for (Integer partitionId : partitions) {
          resPayload.write(ByteBuffer.allocate(4).putInt(partitionId).array());
          List<byte[]> records = getRecords(topicUUID, partitionId);
          resPayload.write(records.isEmpty() ? ERR_UNKNOWN_TOPIC : ERR_NONE);
          resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // high watermark
          resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // last_stable_offset
          resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // log_start_offset
          Utils.putUnsignedVarInt(resPayload, 0); // varint aborted_transactions
          // resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // aborted producer id
          // resPayload.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // aborted first offset
          resPayload.write(new byte[]{0, 0, 0, 0}); // preferred read replica
          Utils.putUnsignedVarInt(resPayload, records.size() + 1);
          for (byte[] record : records) {
            resPayload.write(record);
          }
          resPayload.write((byte) 0); // tag buffer - partition
        }
      }
      resPayload.write((byte) 0); // tag buffer - topic
    }
    resPayload.write((byte) 0); // tag buffer
  }

  static void handleApiVersions(ApiVersionsRequest request, OutputStream resPayload) throws IOException {
    LOG.debug("handleRequest API_KEY_API_VERSIONS");
    resPayload.write(ERR_NONE);
    resPayload.write(4); // num_api_keys + 1
    resPayload.write(ByteBuffer.allocate(2).putShort(Request.API_KEY_API_VERSIONS).array());
    resPayload.write(new byte[]{0, 0}); // min version
    resPayload.write(new byte[]{0, 4}); // max version
    resPayload.write(0); // tag buffer
    resPayload.write(ByteBuffer.allocate(2).putShort(Request.API_KEY_DESCRIBE_TOPIC_PARTITIONS).array());
    resPayload.write(new byte[]{0, 0}); // min version
    resPayload.write(new byte[]{0, 0}); // max version
    resPayload.write(0); // tag buffer
    resPayload.write(ByteBuffer.allocate(2).putShort(Request.API_KEY_FETCH).array());
    resPayload.write(new byte[]{0, 0}); // min version
    resPayload.write(new byte[]{0, 16}); // max version
    resPayload.write(0); // tag buffer
    resPayload.write(new byte[]{0, 0, 0, 0}); // throttle time
    resPayload.write(0); // tag buffer
  }

  static void handleDescribeTopicPartitions(DescribeTopicPartitionsRequest request, OutputStream resPayload) throws IOException {
    LOG.debug("handleRequest API_KEY_DESCRIBE_TOPIC_PARTITIONS");
    resPayload.write(new byte[]{0}); // tag buffer
    resPayload.write(new byte[]{0,0,0,0}); // throttle time

    // topics
    Utils.putUnsignedVarInt(resPayload, request.topicNames.size() + 1); // compact array size
    // for each topic
    for (int i = 0; i < request.topicNames.size(); i++) {
      String topicName = request.topicNames.get(i);
      byte[] topicUUID = Metadata.getInstance().findTopicUUID(topicName);
      if (topicUUID != null) {
        resPayload.write(ERR_NONE);
        Utils.putCompactString(resPayload, topicName);
        resPayload.write(topicUUID); // topic id
        resPayload.write(new byte[]{0}); // is internal
        List<PartitionRecordValue> partitions = Metadata.getInstance().findPartitionRecordValues(topicUUID);
        Utils.putUnsignedVarInt(resPayload, partitions.size() + 1);
        for (PartitionRecordValue partition : partitions) {
          resPayload.write(ERR_NONE);
          resPayload.write(partition.partitionId);
          resPayload.write(partition.leader);
          resPayload.write(partition.leaderEpoch);
          Utils.putUnsignedVarInt(resPayload, partition.replicaArray.size() + 1);
          for (byte[] replica: partition.replicaArray) {
            resPayload.write(replica);
          }
          Utils.putUnsignedVarInt(resPayload, partition.inSyncReplicaArray.size() + 1);
          for (byte[] inSyncReplica: partition.inSyncReplicaArray) {
            resPayload.write(inSyncReplica);
          }
          resPayload.write(new byte[]{1}); // eligible leader replicas
          resPayload.write(new byte[]{1}); // last known eligible leader replicas
          resPayload.write(new byte[]{1}); // last known offline replicas
          resPayload.write(new byte[]{0}); // tag buffer
        }
        resPayload.write(new byte[]{0, 0, 0xD, (byte) 0xF8}); // topic authorized operations
        resPayload.write(new byte[]{0}); // tag buffer
      } else {
        resPayload.write(ERR_UNKNOWN_TOPIC_OR_PARTITION);
        Utils.putCompactString(resPayload, request.topicNames.get(i));
        resPayload.write(new byte[16]); // topic id
        resPayload.write(new byte[]{0}); // is internal
        resPayload.write(new byte[]{1}); // partitions array
        resPayload.write(new byte[]{0, 0, 0xD, (byte) 0xF8}); // topic authorized operations
        resPayload.write(new byte[]{0}); // tag buffer
      }
    }
    resPayload.write(new byte[]{(byte) 0xFF}); // cursor
    resPayload.write(new byte[]{0}); // tag buffer
  }

  static List<byte[]> getRecords(byte[] topicUUID, int partitionId) throws IOException {
    LOG.debug("getRecords topicUUID={} partitionId={}", Utils.bytesToHex(topicUUID), partitionId);
    String topicName = Metadata.getInstance().findTopicName(topicUUID);
    if (topicName != null) {
      TopicPartitionFile file = new TopicPartitionFile(topicName, partitionId);
      return file.getRecords();
    } else {
      return Collections.EMPTY_LIST;
    }
  }
}