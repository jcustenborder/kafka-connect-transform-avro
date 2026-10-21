# kafka-connect-transform-avro

Decode raw Avro binary keys or values into strongly typed Kafka Connect data.
Avro records become Connect `Struct`s; other types use Confluent `AvroData`'s
Connect mappings. This project follows
[kafka-connect-transform-xml](https://github.com/jcustenborder/kafka-connect-transform-xml)'s
build, packaging, and release conventions.

## Configuration

Use `FromAvro$Value` for values or `FromAvro$Key` for keys. Configure exactly one
writer schema source:

| Setting | Description |
| --- | --- |
| `schema.content` | Inline Avro schema JSON. |
| `schema.path` | One `file:`, `http:`, or `https:` URL to an Avro schema. |

Inline example (connector properties):

```properties
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
transforms=fromAvro
transforms.fromAvro.type=com.github.jcustenborder.kafka.connect.transform.avro.FromAvro$Value
transforms.fromAvro.schema.content={"type":"record","name":"Event","namespace":"example","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}
```

To load the schema from a file instead, replace `schema.content` with:

```properties
transforms.fromAvro.schema.path=file:///etc/kafka-connect/schemas/event.avsc
```

For keys, use the `$Key` variant and configure `key.converter` as
`org.apache.kafka.connect.converters.ByteArrayConverter` when reading Kafka bytes.
Source connectors may supply bytes directly, without a byte-array converter.

The payload must contain exactly one **raw Avro binary datum** encoded with the
configured writer schema. Confluent Schema Registry wire framing and Avro object
container files are not supported. No Schema Registry connection is made.
Raw binary does not identify its schema: keep the configured writer schema in
sync with producers; a wrong schema can sometimes decode without an error.
Named schema definitions must be included in the schema document; URL sources
are read once during configuration, not refreshed per message.
URL loading uses 10-second connection and read timeouts.

## Behavior

- Accepts `byte[]` and `ByteBuffer`, with a Connect `BYTES` schema or no schema.
  Only the buffer's remaining bytes are read; its position and limit are preserved.
- Null keys/values pass through unchanged. A binary-encoded null is decoded
  using the configured nullable schema.
- Preserves the unselected side, headers, topic, partition, timestamp, and source
  or sink record metadata.
- Rejects invalid configuration, unsupported input types, truncated data, and
  trailing bytes with explicit exceptions. Empty bytes are valid for schemas
  whose datum consumes no bytes, such as an empty record.
- Uses Confluent's enhanced Avro schema support to retain enum and union names.
  Nullable single-type unions become optional Connect fields; multi-type unions
  use Confluent's union `Struct` representation. Logical types use the mappings
  supported by the installed Confluent converter, including decimal, date,
  time-millis, and timestamp-millis.

## Build and install

Requires Java 17+ and Maven 3.9.2+:

```sh
mvn -B clean verify
```

The build produces the plugin JAR, distribution archives, generated documentation,
and `target/components/packages/jcustenborder-kafka-connect-transform-avro-0.1-SNAPSHOT.zip`.
Unpack the Confluent Hub ZIP into a Kafka Connect plugin directory.

Kafka Connect and **Confluent's Avro converter are provided dependencies**, excluded
from the distribution. The build targets `io.confluent:kafka-connect-avro-converter:8.3.2`.
Make the matching converter and its runtime dependencies visible to this plugin's
classloader, typically by installing them alongside the transform in the same
plugin directory. Merely installing the converter in a separate isolated plugin
directory may not make it visible. Restart the worker after installation.

## Tests and releases

Tests exercise Connect → raw Avro → Connect round trips with schema/value checks,
independent Avro → Connect → Avro semantic round trips, buffer handling, errors,
record metadata, and the actual packaged plugin with worker-provided dependencies.

Pull requests run `mvn -B clean verify` on Java 17. Pushes to `main` use the same
release cycle as the XML project: derive the next `major.minor.patch` version from
Git tags and the POM's `major.minor-SNAPSHOT`, set that version for the build,
run `clean verify`, and publish the Confluent Hub ZIP as a GitHub release asset.
The committed POM remains a snapshot. The first release in this series is `0.1.0`.
