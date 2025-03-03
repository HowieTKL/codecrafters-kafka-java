package org.howietkl.kafka.metadata;

public abstract class RecordValue {
  /*
- Value
  - Frame Version
  - Type
  - Version
   */
  byte frameVersion;
  byte version;

  abstract byte getType();
}
