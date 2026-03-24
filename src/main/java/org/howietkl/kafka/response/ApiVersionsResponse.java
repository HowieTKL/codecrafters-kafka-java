package org.howietkl.kafka.response;

import org.howietkl.kafka.request.ApiVersionsRequest;
import org.howietkl.kafka.request.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ApiVersionsResponse extends Response<ApiVersionsRequest> {
  public static final Logger LOG = LoggerFactory.getLogger(ApiVersionsResponse.class);

  @Override
  public void processResponse(ApiVersionsRequest request, OutputStream out) throws IOException {
    LOG.info("processResponse API_KEY_API_VERSIONS");
    out.write(ERR_NONE);
    out.write(5); // num_api_keys + 1
    out.write(ByteBuffer.allocate(2).putShort(Request.API_KEY_API_VERSIONS).array());
    out.write(new byte[]{0, 0}); // min version
    out.write(new byte[]{0, 4}); // max version
    out.write(0); // tag buffer
    out.write(ByteBuffer.allocate(2).putShort(Request.API_KEY_DESCRIBE_TOPIC_PARTITIONS).array());
    out.write(new byte[]{0, 0}); // min version
    out.write(new byte[]{0, 0}); // max version
    out.write(0); // tag buffer
    out.write(ByteBuffer.allocate(2).putShort(Request.API_KEY_FETCH).array());
    out.write(new byte[]{0, 0}); // min version
    out.write(new byte[]{0, 16}); // max version
    out.write(0); // tag buffer
    out.write(ByteBuffer.allocate(2).putShort(Request.API_KEY_PRODUCE).array());
    out.write(new byte[]{0, 0}); // min version
    out.write(new byte[]{0, 11}); // max version
    out.write(0); // tag buffer
    out.write(new byte[]{0, 0, 0, 0}); // throttle time
    out.write(0); // tag buffer
  }
}