import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FetchRequest extends Request {
  List<byte[]> topicUUIDs = new ArrayList<>();

  @Override
  void parseRequest(ByteBuffer reqPayload) throws IOException {
    System.out.println("parsing FetchRequest");
    reqPayload.getInt(); // max_wait_ms
    reqPayload.getInt(); // min_bytes
    reqPayload.getInt(); // max_bytes
    reqPayload.get();    // isolation_level
    reqPayload.getInt(); // session_id
    reqPayload.getInt(); // session_epoch
    byte topicArrayLength = reqPayload.get();
    for (int i = 0; i < topicArrayLength - 1; i++) {
      byte[] topicUUID = new byte[16];
      reqPayload.get(topicUUID); // topic UUID
      topicUUIDs.add(topicUUID);
      System.out.println(" topicUUID=" + Utils.bytesToHex(topicUUID));
      // partitions
    }
  }
}
