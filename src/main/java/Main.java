import javax.sound.sampled.Port;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  private static final int PORT = 9092;
  private static final int THREADS = 4;
  private static final byte[] ERR_UNSUPPORTED_VERSION = new byte[]{0, 35};
  private static final byte[] ERR_NONE = new byte[]{0, 0};
  private static final byte[] API_KEY_API_VERSIONS = new byte[]{0, 18};
  private static final byte[] API_KEY_DESCRIBE_TOPIC_PARTITIONS = new byte[]{0, 75};

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

  private static void handleRequest(Socket clientSocket) {
System.out.println("handleRequest");
    try (clientSocket) {
      while (!clientSocket.isClosed()) {
        InputStream in = clientSocket.getInputStream();
        byte[] messageSizeBytes = in.readNBytes(4);
        byte[] requestApiKeyBytes = in.readNBytes(2);
        byte[] requestApiVersionBytes = in.readNBytes(2);
        byte[] correlationId = in.readNBytes(4);
        int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();
        System.out.println("handleRequest messageSize=" + messageSize);
        in.skipNBytes(messageSize - 8);
    /*
    message_size => INT32
    correlation_id => INT32
    ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms TAG_BUFFER
      error_code => INT16
      api_keys => api_key min_version max_version TAG_BUFFER
        api_key => INT16
        min_version => INT16
        max_version => INT16
      throttle_time_ms => INT32
     */
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.writeBytes(correlationId);
        short requestApiVersion = ByteBuffer.wrap(requestApiVersionBytes).getShort();
        if (requestApiVersion < 0 || requestApiVersion > 4) {
          payload.write(ERR_UNSUPPORTED_VERSION);
          System.out.println("handleRequest ERR_UNSUPPORTED_VERSION");
        } else {
          System.out.println("handleRequest ERR_NONE");
          payload.write(ERR_NONE);
          payload.write(3);
          payload.write(API_KEY_API_VERSIONS);
          payload.write(new byte[]{0, 0}); // min version
          payload.write(new byte[]{0, 4}); // max version
          payload.write(0); // tagged field
          payload.write(API_KEY_DESCRIBE_TOPIC_PARTITIONS);
          payload.write(new byte[]{0, 0}); // min version
          payload.write(new byte[]{0, 0}); // max version
          payload.write(0); // tagged field
          payload.write(new byte[]{0, 0, 0, 0}); // throttle time
          payload.write(0); // tagged field
        }
        OutputStream out = clientSocket.getOutputStream();
        out.write(ByteBuffer.allocate(messageSizeBytes.length).putInt(payload.size()).array()); // message/payload size
        out.write(payload.toByteArray());
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

}
