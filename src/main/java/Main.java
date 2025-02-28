import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  private static final int PORT = 9092;
  private static final int THREADS = 4;

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
    System.out.println("main system down");
  }

  static void handleRequest(Socket clientSocket) {
    System.out.println("handleRequest");
    try (clientSocket) {
      while (!clientSocket.isClosed()) {
        InputStream in = clientSocket.getInputStream();
        int messageSize = ByteBuffer.wrap(in.readNBytes(4)).getInt();
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
    resPayload.write(3);
    resPayload.write(API_KEY_API_VERSIONS);
    resPayload.write(new byte[]{0, 0}); // min version
    resPayload.write(new byte[]{0, 4}); // max version
    resPayload.write(0); // tag buffer
    resPayload.write(API_KEY_DESCRIBE_TOPIC_PARTITIONS);
    resPayload.write(new byte[]{0, 0}); // min version
    resPayload.write(new byte[]{0, 0}); // max version
    resPayload.write(0); // tag buffer
    resPayload.write(new byte[]{0, 0, 0, 0}); // throttle time
    resPayload.write(0); // tag buffer
  }

  static void handleDescribeTopicPartitions(ByteBuffer reqPayload, ByteArrayOutputStream resPayload) throws IOException {
    System.out.println("handleRequest ERR_UNKNOWN_TOPIC_OR_PARTITION");
    String topicName = getTopicName(reqPayload);
    reqPayload.get(); // tag buffer
    reqPayload.getInt(); // partition limit
    reqPayload.get(); // cursor
    reqPayload.get(); // tag buffer
    resPayload.write(new byte[]{0}); // tag buffer
    resPayload.write(0); // throttle time
    resPayload.write(new byte[]{2}); // array length
    resPayload.write(ERR_UNKNOWN_TOPIC_OR_PARTITION);
    resPayload.write(new byte[]{(byte)(topicName.length() + 1)}); // topic name size
    resPayload.write(topicName.getBytes(StandardCharsets.UTF_8));
    resPayload.write(new byte[16]); // topic id
    resPayload.write(new byte[]{0}); // is internal
    resPayload.write(new byte[]{1}); // partitions array
    resPayload.write(new byte[]{0, 0, 0xD, (byte) 0xF8}); // topic authorized operations
    resPayload.write(new byte[]{0}); // tag buffer
    resPayload.write(new byte[]{(byte) 0xFF}); // cursor
    resPayload.write(new byte[]{0}); // tag buffer
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
      throw new UnsupportedOperationException("handleTopicName size varint");
    }
  }
}
