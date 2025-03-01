import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

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
    ByteArrayInputStream in = new ByteArrayInputStream(new byte[] {0x3c});
    assertEquals(30, Utils.getSignedVarInt(in));

    in = new ByteArrayInputStream(new byte[] {0x01});
    assertEquals(-1, Utils.getSignedVarInt(in));

    in = new ByteArrayInputStream(new byte[] {0x30});
    assertEquals(24, Utils.getSignedVarInt(in));

    in = new ByteArrayInputStream(new byte[] {0x2e});
    assertEquals(23, Utils.getSignedVarInt(in));

    in = new ByteArrayInputStream(new byte[] {0x30});
    assertEquals(24, Utils.getSignedVarInt(in));
  }

  @Test
  void getUnsignedVarInt() throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(new byte[] {7});
    assertEquals(7, Utils.getUnsignedVarInt(in));

    in = new ByteArrayInputStream(new byte[] {-128, 1});
    assertEquals(128, Utils.getUnsignedVarInt(in));
  }

  @Test
  void putUnsignedVarInt() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Utils.putUnsignedVarInt(128, out);
    assertArrayEquals(new byte[]{-128, 1}, out.toByteArray());

    out = new ByteArrayOutputStream();
    Utils.putUnsignedVarInt(129, out);
    assertArrayEquals(new byte[]{-127, 1}, out.toByteArray());

    out = new ByteArrayOutputStream();
    Utils.putUnsignedVarInt(127, out);
    assertArrayEquals(new byte[]{127}, out.toByteArray());
  }
}