import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
  private static final byte[] ERR_UNSUPPORTED_VERSION = new byte[]{0, 35};
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
      OutputStream out = clientSocket.getOutputStream();
      out.write(new byte[]{0, 0, 0, 0});
      out.write(correlationId);
      if (apiVersion < 0 || apiVersion > 4) {
        out.write(ERR_UNSUPPORTED_VERSION);
      }
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
