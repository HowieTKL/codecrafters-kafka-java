package org.howietkl.kafka.request;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ProduceRequest extends Request {
  @Override
  public void parseRequest(ByteBuffer reqPayload) throws IOException {
    super.parseRequest(reqPayload);
  }
}
