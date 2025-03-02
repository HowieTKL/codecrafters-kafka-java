import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class UtilsTest {

  @BeforeEach
  void setUp() {
  }

  @AfterEach
  void tearDown() {
  }

  @Test
  void getSignedVarInt() throws IOException {
    ByteBuffer src = ByteBuffer.wrap(new byte[] {0x3c});
    assertEquals(30, Utils.getSignedVarInt(src));

    src = ByteBuffer.wrap(new byte[] {0x01});
    assertEquals(-1, Utils.getSignedVarInt(src));

    src = ByteBuffer.wrap(new byte[] {0x30});
    assertEquals(24, Utils.getSignedVarInt(src));

    src = ByteBuffer.wrap(new byte[] {0x2e});
    assertEquals(23, Utils.getSignedVarInt(src));

    src = ByteBuffer.wrap(new byte[] {0x30});
    assertEquals(24, Utils.getSignedVarInt(src));
  }

  @Test
  void getUnsignedVarInt() throws IOException {
    ByteBuffer src = ByteBuffer.wrap(new byte[] {7});
    assertEquals(7, Utils.getUnsignedVarInt(src));

    src = ByteBuffer.wrap(new byte[] {-128, 1});
    assertEquals(128, Utils.getUnsignedVarInt(src));
  }

  @Test
  void putUnsignedVarInt() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Utils.putUnsignedVarInt(out, 128);
    assertArrayEquals(new byte[]{-128, 1}, out.toByteArray());

    out = new ByteArrayOutputStream();
    Utils.putUnsignedVarInt(out, 129);
    assertArrayEquals(new byte[]{-127, 1}, out.toByteArray());

    out = new ByteArrayOutputStream();
    Utils.putUnsignedVarInt(out, 127);
    assertArrayEquals(new byte[]{127}, out.toByteArray());
  }

}