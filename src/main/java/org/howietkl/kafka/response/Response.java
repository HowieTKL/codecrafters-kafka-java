package org.howietkl.kafka.response;

import org.howietkl.kafka.request.Request;

import java.io.IOException;
import java.io.OutputStream;

public abstract class Response<T extends Request> {
  public abstract void processResponse(T request, OutputStream out) throws IOException;
}
