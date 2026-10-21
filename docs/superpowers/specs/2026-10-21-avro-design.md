# Raw Avro transformation

Approved design: mirror kafka-connect-transform-xml's Maven build, Java 17 target,
documentation, Confluent Hub packaging, PR verification, and release-on-default-branch
patch versioning. Replace XML mapping with raw Avro decoding. No IDE tooling.

Expose FromAvro$Key and FromAvro$Value in
com.github.jcustenborder.kafka.connect.transform.avro. Configure exactly one of
schema.content (inline Avro JSON) or schema.path (one file/http/https URL).
Parse the writer schema once at configuration. Decode exactly one raw binary
datum using GenericDatumReader and map it using AvroData.toConnectData.
The Confluent converter is a provided dependency; no Schema Registry is contacted.
Enable enhanced Avro schema support to retain enum and union names for round trips.
This repository's default branch is main, so the mirrored release workflow targets main.

Accept schemaful/schemaless byte arrays and ByteBuffers, respecting buffer bounds
without mutating them. Null selected inputs pass through. Preserve the unselected
side and record metadata. Reject invalid configuration with ConfigException and
invalid input/trailing bytes with DataException, retaining causes. Applying before
configuration or after close fails explicitly. Empty bytes can be valid for a
zero-byte datum. Reconfiguration must not retain a previous schema on failure.

Completion requires Connect-to-raw-Avro-to-Connect round trips with schema and value
assertions, plus independently authored Avro fixtures with explicit Connect
expectations and semantic Avro round trips. Cover scalars, nested records,
collections, nullable/multi-type unions, enum, fixed, logical types and supported
Connect metadata. Verify key/value, both schema sources, buffer variants, malformed
input, lifecycle, record metadata, and conversion from the packaged plugin.
Run mvn -B clean verify on Java 17, inspect dependency scope and release packaging,
then create a PR. Wrong writer schemas cannot always be detected by raw Avro.
