import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

class MainTest {

  @org.junit.jupiter.api.BeforeEach
  void setUp() {
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() {
  }

  @Test
  public void testCorrelationId() throws Exception {
    ByteArrayOutputStream resPayload = new ByteArrayOutputStream();
    ByteBuffer reqPayload = ByteBuffer.wrap(new byte[]{0,0, 0,0, 1,1,1,1, 0,1, 97, 0,0});
    Main.handleRequest(reqPayload, resPayload);
    assertArrayEquals(new byte[]{1,1,1,1}, resPayload.toByteArray());
  }

  @Test
  public void testErrUnsupportedVersion() throws Exception {
    ByteArrayOutputStream resPayload = new ByteArrayOutputStream();
    ByteBuffer reqPayload;

    reqPayload = ByteBuffer.wrap(new byte[]{0,0, 0,5, 1,1,1,1, 0,1, 97, 0,0});
    Main.handleRequest(reqPayload, resPayload);
    ByteBuffer resBuffer = ByteBuffer.wrap(resPayload.toByteArray());
    resBuffer.getInt();// correlationId
    assertEquals(ByteBuffer.wrap(Main.ERR_UNSUPPORTED_VERSION).getShort(), resBuffer.getShort());

    reqPayload = ByteBuffer.wrap(new byte[]{0,0, 0,-1, 1,1,1,1, 0,1, 97, 0,0});
    Main.handleRequest(reqPayload, resPayload);
    resBuffer = ByteBuffer.wrap(resPayload.toByteArray());
    resBuffer.getInt();// correlationId
    assertEquals(ByteBuffer.wrap(Main.ERR_UNSUPPORTED_VERSION).getShort(), resBuffer.getShort());
  }



}