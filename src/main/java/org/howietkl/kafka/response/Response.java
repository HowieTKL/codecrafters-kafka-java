package org.howietkl.kafka.response;

import org.howietkl.kafka.request.Request;

import java.io.IOException;
import java.io.OutputStream;

public abstract class Response<T extends Request> {
  public static final byte[] ERR_UNSUPPORTED_VERSION = new byte[]{0, 35};
  public static final byte[] ERR_NONE = new byte[]{0, 0};
  public static final byte[] ERR_UNKNOWN_TOPIC_OR_PARTITION = new byte[]{0, 3};
  public static final byte[] ERR_UNKNOWN_TOPIC = new byte[]{0, 100};

  public abstract void processResponse(T request, OutputStream out) throws IOException;
}
