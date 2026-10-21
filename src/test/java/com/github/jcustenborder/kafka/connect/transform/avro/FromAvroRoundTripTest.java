package com.github.jcustenborder.kafka.connect.transform.avro;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.jcustenborder.kafka.connect.transform.avro.AvroTestData.*;
import static org.junit.jupiter.api.Assertions.*;

class FromAvroRoundTripTest {
  @TempDir
  Path temporary;

  static Stream<Arguments> scalars() {
    return Stream.of(
        Arguments.of(Schema.BOOLEAN_SCHEMA, true),
        Arguments.of(Schema.INT8_SCHEMA, (byte) -123),
        Arguments.of(Schema.INT16_SCHEMA, (short) 32000),
        Arguments.of(Schema.INT32_SCHEMA, -123456789),
        Arguments.of(Schema.INT64_SCHEMA, Long.MAX_VALUE),
        Arguments.of(Schema.FLOAT32_SCHEMA, 1.25f),
        Arguments.of(Schema.FLOAT64_SCHEMA, -123.456d),
        Arguments.of(Schema.STRING_SCHEMA, "Écrivain \uD83D\uDE80"),
        Arguments.of(Schema.BYTES_SCHEMA, new byte[]{0, -1, 42}),
        Arguments.of(Decimal.schema(2), new BigDecimal("-12345.67")),
        Arguments.of(org.apache.kafka.connect.data.Date.SCHEMA, new java.util.Date(86400000L)),
        Arguments.of(org.apache.kafka.connect.data.Time.SCHEMA, new java.util.Date(43200123L)),
        Arguments.of(org.apache.kafka.connect.data.Timestamp.SCHEMA, new java.util.Date(1700000000123L)),
        Arguments.of(SchemaBuilder.array(Schema.INT32_SCHEMA).build(), List.of(1, -2, 3)),
        Arguments.of(SchemaBuilder.array(Schema.STRING_SCHEMA).build(), List.of()),
        Arguments.of(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).build(), Map.of("x", 2L)),
        Arguments.of(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).build(), Map.of()),
        Arguments.of(Schema.OPTIONAL_STRING_SCHEMA, "present"),
        Arguments.of(Schema.OPTIONAL_STRING_SCHEMA, null));
  }

  @ParameterizedTest
  @MethodSource("scalars")
  void connectDataSurvivesRawAvroRoundTrip(Schema schema, Object value) throws Exception {
    AvroData avro = avroData();
    org.apache.avro.Schema writer = avro.fromConnectSchema(schema);
    byte[] bytes = encode(writer, avro.fromConnectData(schema, value));
    try (FromAvro<SinkRecord> transform = new FromAvro.Value<>()) {
      transform.configure(Map.of("schema.content", writer.toString()));
      SinkRecord output = transform.apply(record(false, Schema.BYTES_SCHEMA, bytes));
      assertEquals(schema, output.valueSchema());
      ConnectSchema.validateValue(output.valueSchema(), output.value());
      if (value instanceof byte[]) {
        ByteBuffer actual = output.value() instanceof ByteBuffer
            ? ((ByteBuffer) output.value()).duplicate() : ByteBuffer.wrap((byte[]) output.value());
        assertEquals(ByteBuffer.wrap((byte[]) value), actual);
      } else {
        assertEquals(value, output.value());
      }
      assertEquals(decode(writer, bytes),
          decode(writer, encode(writer, avro.fromConnectData(output.valueSchema(), output.value()))));
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false,false", "false,false,true", "false,true,false", "false,true,true",
      "true,false,false", "true,false,true", "true,true,false", "true,true,true"})
  void nestedConnectRecordRoundTripsAcrossInputs(boolean key, boolean inline, boolean buffer) throws Exception {
    Schema child = SchemaBuilder.struct().name("example.Child").field("id", Schema.INT64_SCHEMA).build();
    Schema schema = SchemaBuilder.struct().name("example.Event").doc("Round-trip event").version(3)
        .parameter("source", "round-trip")
        .field("name", Schema.STRING_SCHEMA)
        .field("child", child)
        .field("children", SchemaBuilder.array(child).build())
        .field("labels", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
        .field("missing", Schema.OPTIONAL_STRING_SCHEMA)
        .field("present", Schema.OPTIONAL_STRING_SCHEMA)
        .field("defaulted", SchemaBuilder.string().defaultValue("fallback").build())
        .build();
    Struct nested = new Struct(child).put("id", 42L);
    Struct value = new Struct(schema).put("name", "Événement").put("child", nested)
        .put("children", List.of(nested)).put("labels", Map.of("region", "west"))
        .put("missing", null).put("present", "yes");
    AvroData avro = avroData();
    org.apache.avro.Schema writer = avro.fromConnectSchema(schema);
    byte[] bytes = encode(writer, avro.fromConnectData(schema, value));
    Map<String, String> config = inline ? Map.of("schema.content", writer.toString())
        : Map.of("schema.path", Files.writeString(temporary.resolve("event.avsc"), writer.toString()).toUri().toString());
    try (FromAvro<SinkRecord> transform = key ? new FromAvro.Key<>() : new FromAvro.Value<>()) {
      transform.configure(config);
      SinkRecord output = transform.apply(record(key, null, buffer ? ByteBuffer.wrap(bytes) : bytes));
      Schema actualSchema = key ? output.keySchema() : output.valueSchema();
      Object actual = key ? output.key() : output.value();
      assertEquals(schema, actualSchema);
      // Struct.equals compares raw slots; Avro materializes the default that Struct.get resolves lazily.
      for (org.apache.kafka.connect.data.Field field : schema.fields()) {
        assertEquals(value.get(field), ((Struct) actual).get(field), field.name());
      }
      assertEquals("fallback", ((Struct) actual).getString("defaulted"));
      ConnectSchema.validateValue(actualSchema, actual);
      GenericRecord decoded = (GenericRecord) decode(writer,
          encode(writer, avro.fromConnectData(actualSchema, actual)));
      assertEquals("Événement", decoded.get("name").toString());
      assertEquals(42L, ((GenericRecord) decoded.get("child")).get("id"));
    }
  }

  @ParameterizedTest
  @MethodSource("unionValues")
  void independentAvroFixtureHasExpectedConnectTypesAndRoundTrips(Object choice) throws Exception {
    org.apache.avro.Schema writer = new org.apache.avro.Schema.Parser().parse("""
        {"type":"record","name":"Independent","namespace":"example","fields":[
          {"name":"id","type":"int"},
          {"name":"name","type":"string"},
          {"name":"numbers","type":{"type":"array","items":"long"}},
          {"name":"labels","type":{"type":"map","values":"string"}},
          {"name":"status","type":{"type":"enum","name":"Status","symbols":["OPEN","CLOSED"]}},
          {"name":"token","type":{"type":"fixed","name":"Token","size":3}},
          {"name":"choice","type":["null","long","string"]},
          {"name":"note","type":["null","string"],"default":null}
        ]}
        """);
    GenericRecord original = new GenericData.Record(writer);
    original.put("id", 7);
    original.put("name", "independent");
    original.put("numbers", List.of(1L, 2L));
    original.put("labels", Map.of("team", "connect"));
    original.put("status", new GenericData.EnumSymbol(writer.getField("status").schema(), "OPEN"));
    original.put("token", new GenericData.Fixed(writer.getField("token").schema(), new byte[]{1, 2, 3}));
    original.put("choice", choice);
    original.put("note", null);
    byte[] bytes = encode(writer, original);
    try (FromAvro<SinkRecord> transform = new FromAvro.Value<>()) {
      transform.configure(Map.of("schema.content", writer.toString()));
      SinkRecord result = transform.apply(record(false, Schema.BYTES_SCHEMA, bytes));
      Struct actual = (Struct) result.value();
      assertEquals("example.Independent", actual.schema().name());
      assertEquals(Schema.Type.INT32, actual.schema().field("id").schema().type());
      assertEquals(7, actual.getInt32("id"));
      assertEquals("independent", actual.getString("name"));
      assertEquals(List.of(1L, 2L), actual.getArray("numbers"));
      assertEquals(Map.of("team", "connect"), actual.getMap("labels"));
      assertEquals("OPEN", actual.getString("status"));
      assertEquals(Schema.Type.STRING, actual.schema().field("status").schema().type());
      assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 3}), actual.get("token"));
      assertEquals(Schema.Type.BYTES, actual.schema().field("token").schema().type());
      assertTrue(actual.schema().field("choice").schema().isOptional());
      if (choice == null) {
        assertNull(actual.get("choice"));
      } else {
        Struct union = actual.getStruct("choice");
        assertEquals(choice instanceof Long ? choice : null, union.get("long"));
        assertEquals(choice instanceof String ? choice : null, union.get("string"));
      }
      assertNull(actual.get("note"));
      assertTrue(actual.schema().field("note").schema().isOptional());
      ConnectSchema.validateValue(result.valueSchema(), actual);
      Object reversed = avroData().fromConnectData(result.valueSchema(), actual);
      Object decoded = decode(writer, encode(writer, reversed));
      // Compare semantic data, not bytes: map iteration order is not a wire-format contract.
      assertEquals(decode(writer, bytes), decoded);
    }
  }

  static Stream<Arguments> unionValues() {
    return Stream.of(Arguments.of((Object) null), Arguments.of(42L), Arguments.of("selected"));
  }

  @Test
  void independentNativeAvroLogicalTypesRoundTrip() throws Exception {
    org.apache.avro.Schema writer = new org.apache.avro.Schema.Parser().parse("""
        {"type":"record","name":"LogicalEvent","fields":[
          {"name":"day","type":{"type":"int","logicalType":"date"}},
          {"name":"time","type":{"type":"int","logicalType":"time-millis"}},
          {"name":"timestamp","type":{"type":"long","logicalType":"timestamp-millis"}},
          {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}}
        ]}
        """);
    GenericRecord original = new GenericData.Record(writer);
    original.put("day", 1);
    original.put("time", 43200123);
    original.put("timestamp", 1700000000123L);
    original.put("amount", ByteBuffer.wrap(new byte[]{0x30, 0x39}));
    byte[] bytes = encode(writer, original);
    try (FromAvro<SinkRecord> transform = new FromAvro.Value<>()) {
      transform.configure(Map.of("schema.content", writer.toString()));
      SinkRecord output = transform.apply(record(false, Schema.BYTES_SCHEMA, bytes));
      Struct actual = (Struct) output.value();
      assertEquals(org.apache.kafka.connect.data.Date.SCHEMA, actual.schema().field("day").schema());
      assertEquals(org.apache.kafka.connect.data.Time.SCHEMA, actual.schema().field("time").schema());
      assertEquals(org.apache.kafka.connect.data.Timestamp.SCHEMA, actual.schema().field("timestamp").schema());
      Schema decimal = actual.schema().field("amount").schema();
      assertEquals(Decimal.LOGICAL_NAME, decimal.name());
      assertEquals("2", decimal.parameters().get(Decimal.SCALE_FIELD));
      assertEquals("9", decimal.parameters().get("connect.decimal.precision"));
      assertEquals(new java.util.Date(86400000L), actual.get("day"));
      assertEquals(new java.util.Date(43200123L), actual.get("time"));
      assertEquals(new java.util.Date(1700000000123L), actual.get("timestamp"));
      assertEquals(new BigDecimal("123.45"), actual.get("amount"));
      ConnectSchema.validateValue(output.valueSchema(), actual);
      assertEquals(decode(writer, bytes),
          decode(writer, encode(writer, avroData().fromConnectData(output.valueSchema(), actual))));
    }
  }
}
