package com.github.jcustenborder.kafka.connect.transform.avro;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

final class AvroTestData {
  private AvroTestData() {
  }

  static AvroData avroData() {
    return new AvroData(new AvroDataConfig(Map.of(
        AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)));
  }

  static byte[] encode(org.apache.avro.Schema schema, Object datum) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
    new GenericDatumWriter<Object>(schema).write(unwrap(datum), encoder);
    encoder.flush();
    return output.toByteArray();
  }

  static Object decode(org.apache.avro.Schema schema, byte[] bytes) throws IOException {
    return new GenericDatumReader<Object>(schema).read(null,
        DecoderFactory.get().binaryDecoder(bytes, null));
  }

  static Object unwrap(Object datum) {
    return datum instanceof NonRecordContainer ? ((NonRecordContainer) datum).getValue() : datum;
  }

  static SinkRecord record(boolean key, Schema schema, Object value) {
    SinkRecord record = new SinkRecord("events", 2,
        key ? schema : Schema.STRING_SCHEMA, key ? value : "keep-key",
        key ? Schema.STRING_SCHEMA : schema, key ? "keep-value" : value,
        42L, 123456789L, TimestampType.CREATE_TIME);
    record.headers().addString("trace-id", "abc");
    return record;
  }
}
