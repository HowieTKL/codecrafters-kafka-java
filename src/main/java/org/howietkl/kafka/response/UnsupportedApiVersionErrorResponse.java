package org.howietkl.kafka.response;

import org.howietkl.kafka.request.UnsupportedApiVersionErrorRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class UnsupportedApiVersionErrorResponse extends Response<UnsupportedApiVersionErrorRequest> {
  public static final Logger LOG = LoggerFactory.getLogger(UnsupportedApiVersionErrorResponse.class);

  @Override
  public void processResponse(UnsupportedApiVersionErrorRequest request, OutputStream out) throws IOException {
    LOG.info("processResponse ERR_UNSUPPORTED_VERSION");
    out.write(ERR_UNSUPPORTED_VERSION);
  }
}
