package org.howietkl.kafka;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Utils {

  public static void putCompactString(OutputStream out, String string) throws IOException {
    putUnsignedVarInt(out, string.length() + 1);
    out.write(string.getBytes(StandardCharsets.UTF_8));
  }

  public static String getCompactString(ByteBuffer src) throws IOException {
    int size = getUnsignedVarInt(src) - 1;
    byte[] stringBytes = new byte[size];
    src.get(stringBytes);
    return new String(stringBytes, StandardCharsets.UTF_8);
  }

  public static void putCompactArray(OutputStream out, List<byte[]> array) throws IOException {
    putUnsignedVarInt(out, array.size() + 1);
    for (byte[] a : array) {
      out.write(a);
    }
  }

  public static int getSignedVarInt(ByteBuffer src) throws IOException {
    int value = getUnsignedVarInt(src);
    return (value >>> 1) ^ -(value & 1);
  }

  /* Adapted from
   * https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/util/VarInt.java
   */
  public static int getUnsignedVarInt(ByteBuffer src) throws IOException {
    int tmp;
    if ((tmp = src.get()) >= 0) {
      return tmp;
    }
    int result = tmp & 0x7f;
    if ((tmp = src.get()) >= 0) {
      result |= tmp << 7;
    } else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = src.get()) >= 0) {
        result |= tmp << 14;
      } else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = src.get()) >= 0) {
          result |= tmp << 21;
        } else {
          result |= (tmp & 0x7f) << 21;
          result |= (tmp = src.get()) << 28;
          if (tmp < 0) {
            throw new IllegalArgumentException("varint too large");
          }
        }
      }
    }
    return result;
  }
  public static void putUnsignedVarInt(OutputStream outputStream, int v) throws IOException {
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

  public static String bytesToHex(byte[] bytes) {
    StringBuilder hexString = new StringBuilder();
    for (byte b : bytes) {
      String hex = String.format("%02x", b);
      hexString.append(hex);
    }
    return hexString.toString();
  }

}
