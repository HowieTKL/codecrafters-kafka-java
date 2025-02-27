import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
  private static final byte[] ERR_UNSUPPORTED_VERSION = new byte[]{0, 35};
  private static final byte[] ERR_NONE = new byte[]{0, 0};
  private static final byte[] API_KEY_API_VERSIONS = new byte[]{0, 0, 0, 18};
  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 9092;
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      clientSocket = serverSocket.accept();
      InputStream in = clientSocket.getInputStream();
      byte[] messageSize = in.readNBytes(4);
      byte[] requestApiKey = in.readNBytes(2);
      byte[] requestApiVersion = in.readNBytes(2);
      byte[] correlationId = in.readNBytes(4);
      short apiVersion = ByteBuffer.wrap(requestApiVersion).getShort();
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
      if (apiVersion < 0 || apiVersion > 4) {
        payload.write(ERR_UNSUPPORTED_VERSION);
      } else {
        payload.write(ERR_NONE);
      }
      payload.write(2);
      payload.write(API_KEY_API_VERSIONS);
      payload.write(new byte[]{0, 3}); // min version
      payload.write(new byte[]{0, 4}); // max version
      payload.write(0); // tagged field
      payload.write(new byte[]{0, 0, 0, 0}); // throttle time
      payload.write(0); // tagged field

      OutputStream out = clientSocket.getOutputStream();
      out.write(ByteBuffer.allocate(4).putInt(payload.size()).array()); // message/payload size
      out.write(payload.toByteArray());
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}
