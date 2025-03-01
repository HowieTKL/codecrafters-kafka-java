import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

class Utils {

  static void writeCompactString(OutputStream out, String string) throws IOException {
    putUnsignedVarInt(string.length() + 1, out);
    out.write(string.getBytes(StandardCharsets.UTF_8));
  }

  static int getSignedVarInt(InputStream inputStream) throws IOException {
    int value = getUnsignedVarInt(inputStream);
    return (value >>> 1) ^ -(value & 1);
  }

  /* Adapted from
   * https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/util/VarInt.java
   */
  static int getUnsignedVarInt(InputStream src) throws IOException {
    int tmp;
    if ((tmp = src.read()) >= 0) {
      return tmp;
    }
    int result = tmp & 0x7f;
    if ((tmp = src.read()) >= 0) {
      result |= tmp << 7;
    } else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = src.read()) >= 0) {
        result |= tmp << 14;
      } else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = src.read()) >= 0) {
          result |= tmp << 21;
        } else {
          result |= (tmp & 0x7f) << 21;
          result |= (tmp = src.read()) << 28;
          if (tmp < 0) {
            throw new IllegalArgumentException("varint too large");
          }
        }
      }
    }
    return result;
  }
  static void putUnsignedVarInt(int v, OutputStream outputStream) throws IOException {
    byte[] bytes = new byte[unsignedVarIntSize(v)];
    putUnsignedVarInt(v, bytes, 0);
    outputStream.write(bytes);
  }
  private static int putUnsignedVarInt(int v, byte[] sink, int offset) {
    do {
      // Encode next 7 bits + terminator bit
      int bits = v & 0x7F;
      v >>>= 7;
      byte b = (byte) (bits + ((v != 0) ? 0x80 : 0));
      sink[offset++] = b;
    } while (v != 0);
    return offset;
  }
  private static int unsignedVarIntSize(int i) {
    int result = 0;
    do {
      result++;
      i >>>= 7;
    } while (i != 0);
    return result;
  }

}
