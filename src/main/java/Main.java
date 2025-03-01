import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  private static final int PORT = 9092;
  private static final int THREADS = 4;

  private static final String KAFKA_CLUSTER_METADATA_LOG_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

  static final short API_KEY_API_VERSIONS = 18;
  static final short API_KEY_DESCRIBE_TOPIC_PARTITIONS = 75;

  static final int OFFSET_API_KEY = 0;
  static final int OFFSET_API_VERSION = 2;
  static final int OFFSET_CORRELATION_ID = 4;
  static final int OFFSET_CLIENT_ID_SIZE = 8;
  static final int OFFSET_CLIENT_ID = 10;

  static final byte[] ERR_UNSUPPORTED_VERSION = new byte[]{0, 35};
  static final byte[] ERR_NONE = new byte[]{0, 0};
  static final byte[] ERR_UNKNOWN_TOPIC_OR_PARTITION = new byte[]{0, 3};

  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
    try (ServerSocket serverSocket = new ServerSocket(PORT)) {
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      while (true) {
        System.out.println("main waiting for clients");
        // Wait for connection from client.
        Socket clientSocket = serverSocket.accept();
        executorService.submit(() -> handleRequest(clientSocket));
      }
    } catch (IOException e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    } finally {
      executorService.close();
    }
    System.out.println("main system terminated");
  }

  static void handleRequest(Socket clientSocket) {
    System.out.println("handleRequest");
    try (clientSocket) {
      InputStream in = clientSocket.getInputStream();
      while (true) {
        byte[] messageSizeBytes = new byte[4];
        if (in.read(messageSizeBytes) <= 0) {
          break;
        }
        int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();
        System.out.println("handleRequest messageSize=" + messageSize);
        ByteBuffer reqPayload = ByteBuffer.wrap(in.readNBytes(messageSize));
        ByteArrayOutputStream resPayload = new ByteArrayOutputStream();
        handleRequest(reqPayload, resPayload);
        OutputStream out = clientSocket.getOutputStream();
        out.write(ByteBuffer.allocate(4).putInt(resPayload.size()).array()); // message/payload size
        out.write(resPayload.toByteArray());
        out.flush();
        System.out.println("handleRequest flushed response");
      }
    } catch (IOException e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    }
  }

  static void handleRequest(ByteBuffer reqPayload, ByteArrayOutputStream resPayload) throws IOException {
    short requestApiKey = reqPayload.getShort();
    short requestApiVersion = reqPayload.getShort();
    byte[] correlationIdBytes = new byte[4];
    reqPayload.get(correlationIdBytes);
    resPayload.write(correlationIdBytes);
    getClientId(reqPayload);
    reqPayload.get(); // tag buffer
    reqPayload.get(); // array length
    if (requestApiVersion < 0 || requestApiVersion > 4) {
      resPayload.write(ERR_UNSUPPORTED_VERSION);
      System.out.println("handleRequest ERR_UNSUPPORTED_VERSION");
    } else if (requestApiKey == API_KEY_API_VERSIONS) {
      handleApiVersions(reqPayload, resPayload);
    } else if (requestApiKey == API_KEY_DESCRIBE_TOPIC_PARTITIONS) {
      handleDescribeTopicPartitions(reqPayload, resPayload);
    }
  }

  static void handleApiVersions(ByteBuffer reqPayload, ByteArrayOutputStream resPayload) throws IOException {
    System.out.println("handleRequest API_KEY_API_VERSIONS");
    resPayload.write(ERR_NONE);
    resPayload.write(3); // num_api_keys + 1
    resPayload.write(ByteBuffer.allocate(2).putShort(API_KEY_API_VERSIONS).array());
    resPayload.write(new byte[]{0, 0}); // min version
    resPayload.write(new byte[]{0, 4}); // max version
    resPayload.write(0); // tag buffer
    resPayload.write(ByteBuffer.allocate(2).putShort(API_KEY_DESCRIBE_TOPIC_PARTITIONS).array());
    resPayload.write(new byte[]{0, 0}); // min version
    resPayload.write(new byte[]{0, 0}); // max version
    resPayload.write(0); // tag buffer
    resPayload.write(new byte[]{0, 0, 0, 0}); // throttle time
    resPayload.write(0); // tag buffer
  }

  static void handleDescribeTopicPartitions(ByteBuffer reqPayload, ByteArrayOutputStream resPayload) throws IOException {
    List<RecordBatch> recordBatches = getMetadataLog();
    System.out.println("handleRequest ERR_UNKNOWN_TOPIC_OR_PARTITION");
    String topicName = getTopicName(reqPayload);
    reqPayload.get(); // tag buffer
    reqPayload.getInt(); // partition limit
    reqPayload.get(); // cursor
    reqPayload.get(); // tag buffer
    resPayload.write(new byte[]{0}); // tag buffer
    resPayload.write(new byte[]{0,0,0,0}); // throttle time
    resPayload.write(new byte[]{2}); // array length
    resPayload.write(ERR_UNKNOWN_TOPIC_OR_PARTITION);
    Utils.writeCompactString(resPayload, topicName);
    resPayload.write(new byte[16]); // topic id
    resPayload.write(new byte[]{0}); // is internal
    resPayload.write(new byte[]{1}); // partitions array
    resPayload.write(new byte[]{0, 0, 0xD, (byte) 0xF8}); // topic authorized operations
    resPayload.write(new byte[]{0}); // tag buffer
    resPayload.write(new byte[]{(byte) 0xFF}); // cursor
    resPayload.write(new byte[]{0}); // tag buffer
  }

  static List<RecordBatch> getMetadataLog() throws IOException {
    List<RecordBatch> recordBatches = new ArrayList<>();
    try (FileInputStream fis = new FileInputStream(KAFKA_CLUSTER_METADATA_LOG_PATH)) {
      while (fis.available() > 0) {
        recordBatches.add(getRecordBatch(fis));
      }
    }
    return recordBatches;
  }

  static RecordBatch getRecordBatch(FileInputStream fis) throws IOException {
    RecordBatch recBatch = new RecordBatch();
    recBatch.baseOffset = fis.readNBytes(8);
    recBatch.batchLength = fis.readNBytes(4);
    recBatch.partitionLeaderEpoch = fis.readNBytes(4);
    recBatch.magicByte = fis.readNBytes(1);
    recBatch.crc = fis.readNBytes(4);
    recBatch.attributes = fis.readNBytes(2);
    recBatch.lastOffsetDelta = fis.readNBytes(4);
    recBatch.baseTimestamp = fis.readNBytes(8);
    recBatch.maxTimestamp = fis.readNBytes(8);
    recBatch.producerId = fis.readNBytes(8);
    recBatch.producerEpoch = fis.readNBytes(2);
    recBatch.baseSequence = fis.readNBytes(4);
    recBatch.records = getRecords(fis);
    return recBatch;
  }

  static List<Record> getRecords(FileInputStream fis) throws IOException {
    List<Record> records = new ArrayList<>();
    int numRecords = ByteBuffer.wrap(fis.readNBytes(4)).getInt();
    for (int i = 0; i < numRecords; i++) {
      records.add(getRecord(fis));
    }
    return records;
  }

  static Record getRecord(FileInputStream fis) throws IOException {
    Record record = new Record();
    record.length = Utils.getSignedVarInt(fis);
    record.attributes = (byte) fis.read(); // attributes (unused)
    record.timestamp = Utils.getSignedVarInt(fis);
    record.offset = Utils.getSignedVarInt(fis);
    record.keyLength = Utils.getSignedVarInt(fis);
    if (record.keyLength > 0) {
      record.key = fis.readNBytes(record.keyLength);
    }
    record.valueLength = Utils.getSignedVarInt(fis);
    if (record.valueLength > 0) {
      byte frameVersion = (byte) fis.read();
      int type = fis.read();
      switch (type) {
        case TopicRecordValue.TYPE -> record.recordValue = getTopicRecordValue(fis);
        case PartitionRecordValue.TYPE -> record.recordValue = getPartitionRecordValue(fis);
        case FeatureLevelRecordValue.TYPE -> record.recordValue = getFeatureLevelRecordValue(fis);
      }
      record.recordValue.frameVersion = frameVersion;
      record.headersArrayCount = Utils.getUnsignedVarInt(fis);
    }
    return record;
  }

  static TopicRecordValue getTopicRecordValue(FileInputStream fis) throws IOException {
    TopicRecordValue recordValue = new TopicRecordValue();
    // todo
    return recordValue;
  }

  static PartitionRecordValue getPartitionRecordValue(FileInputStream fis) throws IOException {
    PartitionRecordValue recordValue = new PartitionRecordValue();
    // todo
    return recordValue;
  }

  static FeatureLevelRecordValue getFeatureLevelRecordValue(FileInputStream fis) throws IOException {
    FeatureLevelRecordValue recordValue = new FeatureLevelRecordValue();
    // todo
    return recordValue;
  }

  // set clientId and returns reqPayload index AFTER the clientId
  static String getClientId(ByteBuffer reqPayload) {
    short clientIdSize = reqPayload.getShort();
    byte[] clientIdBytes = new byte[clientIdSize];
    reqPayload.get(clientIdBytes);
    return new String(clientIdBytes, StandardCharsets.UTF_8);
  }

  static String getTopicName(ByteBuffer reqPayload) {
    byte size = reqPayload.get();// topic name size

    if (size > 0) {
      --size; // as per spec, we subtract 1
      byte[] topicNameBytes = new byte[size];
      reqPayload.get(topicNameBytes);
      return new String(topicNameBytes, StandardCharsets.UTF_8);
    } else {
         throw new IllegalArgumentException("handleTopicName size varint");
    }
  }

}
