package com.github.jcustenborder.kafka.connect.transform.avro;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.github.jcustenborder.kafka.connect.transform.avro.AvroTestData.record;
import static org.junit.jupiter.api.Assertions.*;

class FromAvroTest {
  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  void changesOnlySelectedSideAndPreservesSinkMetadata(boolean key, boolean schemaful) {
    try (FromAvro<SinkRecord> transform = key ? new FromAvro.Key<>() : new FromAvro.Value<>()) {
      transform.configure(Map.of("schema.content", "\"long\""));
      SinkRecord original = record(key, schemaful ? Schema.BYTES_SCHEMA : null, new byte[]{84});
      SinkRecord result = transform.apply(original);
      assertEquals(42L, key ? result.key() : result.value());
      assertEquals(Schema.INT64_SCHEMA, key ? result.keySchema() : result.valueSchema());
      assertSame(key ? original.value() : original.key(), key ? result.value() : result.key());
      assertSame(key ? original.valueSchema() : original.keySchema(), key ? result.valueSchema() : result.keySchema());
      assertArrayEquals(new byte[]{84}, (byte[]) (key ? original.key() : original.value()));
      assertEquals(original.topic(), result.topic());
      assertEquals(original.kafkaPartition(), result.kafkaPartition());
      assertEquals(original.kafkaOffset(), result.kafkaOffset());
      assertEquals(original.timestamp(), result.timestamp());
      assertEquals(original.timestampType(), result.timestampType());
      assertEquals("abc", result.headers().lastWithName("trace-id").value());
    }
  }

  @Test
  void preservesSourceMetadata() {
    try (FromAvro<SourceRecord> transform = new FromAvro.Value<>()) {
      transform.configure(Map.of("schema.content", "\"long\""));
      SourceRecord original = new SourceRecord(Map.of("file", "events"), Map.of("position", 123),
          "events", 2, Schema.STRING_SCHEMA, "key", Schema.BYTES_SCHEMA, new byte[]{84}, 456L);
      original.headers().addString("trace-id", "source");
      SourceRecord output = transform.apply(original);
      assertEquals(42L, output.value());
      assertEquals(original.sourcePartition(), output.sourcePartition());
      assertEquals(original.sourceOffset(), output.sourceOffset());
      assertEquals(original.topic(), output.topic());
      assertEquals(original.kafkaPartition(), output.kafkaPartition());
      assertEquals(original.timestamp(), output.timestamp());
      assertSame(original.key(), output.key());
      assertEquals("source", output.headers().lastWithName("trace-id").value());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nullSelectedSidePassesThrough(boolean key) {
    try (FromAvro<SinkRecord> transform = key ? new FromAvro.Key<>() : new FromAvro.Value<>()) {
      transform.configure(Map.of("schema.content", "\"long\""));
      for (Schema schema : Arrays.asList(null, Schema.OPTIONAL_BYTES_SCHEMA)) {
        SinkRecord original = record(key, schema, null);
        assertSame(original, transform.apply(original));
      }
    }
  }

  @Test
  void respectsBufferBoundsWithoutMutatingPositionOrLimit() {
    ByteBuffer heap = ByteBuffer.wrap(new byte[]{99, 84, 99});
    heap.position(1).limit(2);
    ByteBuffer direct = ByteBuffer.allocateDirect(3);
    direct.put(new byte[]{99, 84, 99}).position(1).limit(2);
    for (ByteBuffer input : List.of(heap, heap.slice(), heap.asReadOnlyBuffer(), direct)) {
      try (FromAvro<SinkRecord> transform = new FromAvro.Value<>()) {
        transform.configure(Map.of("schema.content", "\"long\""));
        int position = input.position();
        int limit = input.limit();
        assertEquals(42L, transform.apply(record(false, Schema.BYTES_SCHEMA, input)).value());
        assertEquals(position, input.position());
        assertEquals(limit, input.limit());
      }
    }
  }

  @Test
  void rejectsMalformedAndTrailingDataWithCause() {
    try (FromAvro<SinkRecord> transform = new FromAvro.Value<>()) {
      transform.configure(Map.of("schema.content", "\"long\""));
      for (byte[] bytes : List.of(new byte[0], new byte[]{(byte) 0x80}, new byte[]{84, 0})) {
        DataException error = assertThrows(DataException.class,
            () -> transform.apply(record(false, Schema.BYTES_SCHEMA, bytes)));
        assertNotNull(error.getCause());
      }
      transform.configure(Map.of("schema.content", "[\"null\",\"string\"]"));
      assertThrows(DataException.class,
          () -> transform.apply(record(false, Schema.BYTES_SCHEMA, new byte[]{4})));
    }
  }

  @Test
  void rejectsUnsupportedInputAndIncompatibleDeclaredSchema() {
    try (FromAvro<SinkRecord> transform = new FromAvro.Value<>()) {
      transform.configure(Map.of("schema.content", "\"long\""));
      assertThrows(DataException.class, () -> transform.apply(record(false, null, "bytes")));
      assertThrows(DataException.class, () -> transform.apply(record(false, null, 42)));
      assertThrows(DataException.class, () -> transform.apply(record(false, Schema.STRING_SCHEMA, new byte[]{84})));
      assertThrows(DataException.class, () -> transform.apply(record(false, Schema.BYTES_SCHEMA, "bytes")));
    }
  }

  @Test
  void acceptsZeroByteEmptyRecord() {
    try (FromAvro<SinkRecord> transform = new FromAvro.Value<>()) {
      transform.configure(Map.of("schema.content", "{\"type\":\"record\",\"name\":\"Empty\",\"fields\":[]}"));
      Struct result = (Struct) transform.apply(record(false, null, new byte[0])).value();
      assertEquals("Empty", result.schema().name());
      assertTrue(result.schema().fields().isEmpty());
    }
  }

  @Test
  void lifecycleAndFailedReconfigurationDoNotReuseOldSchema() {
    try (FromAvro<SinkRecord> transform = new FromAvro.Value<>()) {
      SinkRecord input = record(false, null, new byte[]{84});
      assertThrows(IllegalStateException.class, () -> transform.apply(input));
      transform.configure(Map.of("schema.content", "\"long\""));
      assertEquals(42L, transform.apply(input).value());
      assertThrows(ConfigException.class, () -> transform.configure(Map.of("schema.content", "broken")));
      assertThrows(IllegalStateException.class, () -> transform.apply(input));
      transform.configure(Map.of("schema.content", "\"int\""));
      assertEquals(42, transform.apply(input).value());
      transform.close();
      transform.close();
      assertThrows(IllegalStateException.class, () -> transform.apply(input));
    }
  }
}
