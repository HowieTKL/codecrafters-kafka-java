package org.howietkl.kafka.metadata;

public class Record {
    /*
- Length: varint
- Attributes: 1 byte
- Timestamp Delta: varint
- Offset : varint
- Key Length: signed varint
- Key:
- Value Length: varint
- Value
- Headers Array Count
     */
  int length; // signed varint
  byte attributes; // 1 byte
  int timestamp; // signed varint
  int offset; // signed varint
  int keyLength; // signed varint
  byte[] key;
  int valueLength; // signed varint
  RecordValue recordValue;
  int headersArrayCount; // unsigned varint
}
