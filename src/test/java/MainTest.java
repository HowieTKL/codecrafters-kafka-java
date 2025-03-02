import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MainTest {

  @org.junit.jupiter.api.BeforeEach
  void setUp() {
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() {
  }

  @Test
  public void testApiVersionsRequest() throws Exception {
    ByteArrayOutputStream resPayload = new ByteArrayOutputStream();
    ByteBuffer reqPayload = ByteBuffer.wrap(new byte[]{0,18, 0,0, 0,0,1,1, 0,1, 97, 0,0});
    Main.handleRequest(reqPayload, resPayload);
    ByteBuffer resBuffer = ByteBuffer.wrap(resPayload.toByteArray());
    byte[] correlationId = new byte[4];
    resBuffer.get(correlationId);
    assertArrayEquals(new byte[]{0,0,1,1}, correlationId); // correlationId
    assertEquals(0, resBuffer.getShort()); // ERR_NONE
  }

  @Test
  public void testErrUnsupportedVersion() throws Exception {
    ByteArrayOutputStream resPayload = new ByteArrayOutputStream();
    ByteBuffer reqPayload = ByteBuffer.wrap(new byte[]{0,18, 0,5, 1,1,1,1, 0,1, 97, 0,0});
    Main.handleRequest(reqPayload, resPayload);
    ByteBuffer resBuffer = ByteBuffer.wrap(resPayload.toByteArray());
    byte[] correlationId = new byte[4];
    resBuffer.get(correlationId);
    assertArrayEquals(new byte[]{1,1,1,1}, correlationId); // correlationId
    assertEquals(ByteBuffer.wrap(Main.ERR_UNSUPPORTED_VERSION).getShort(), resBuffer.getShort());

    resPayload = new ByteArrayOutputStream();
    reqPayload = ByteBuffer.wrap(new byte[]{0,18, 0,-1, 1,0,0,1, 0,1, 97, 0,0});
    Main.handleRequest(reqPayload, resPayload);
    resBuffer = ByteBuffer.wrap(resPayload.toByteArray());
    correlationId = new byte[4];
    resBuffer.get(correlationId);
    assertArrayEquals(new byte[]{1,0,0,1}, correlationId); // correlationId
    assertEquals(ByteBuffer.wrap(Main.ERR_UNSUPPORTED_VERSION).getShort(), resBuffer.getShort());
  }
 }