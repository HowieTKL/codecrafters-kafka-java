import org.howietkl.kafka.request.*;
import org.howietkl.kafka.response.ApiVersionsResponse;
import org.howietkl.kafka.response.DescribeTopicPartitionsResponse;
import org.howietkl.kafka.response.FetchResponse;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  private static final int PORT = 9092;
  private static final int THREADS = 4;

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
          new ApiVersionsResponse().processResponse(apiVersionsRequest, resPayload);
      case DescribeTopicPartitionsRequest describeTopicPartitionsRequest ->
          new DescribeTopicPartitionsResponse().processResponse(describeTopicPartitionsRequest, resPayload);
      case FetchRequest fetchRequest ->
          new FetchResponse().processResponse(fetchRequest, resPayload);
      default -> {
        LOG.error("Unsupported request type={}", request.getClass().getName());
        throw new UnsupportedOperationException("Unsupported request type: " + request);
      }
    }
  }

}