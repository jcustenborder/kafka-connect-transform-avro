====================
Avro Transformations
====================

Convert raw Avro binary keys or values into strongly typed Kafka Connect data.
Configure exactly one writer schema using ``schema.content`` (inline Avro JSON)
or ``schema.path`` (one file, HTTP, or HTTPS URL).

Confluent's Avro converter and its runtime dependencies must be available on the
plugin classpath. This transformation does not contact Schema Registry and does
not accept Confluent wire framing or Avro object container files.

.. toctree::
    :maxdepth: 1
    :caption: Transformations:
    :hidden:
    :glob:

    transformations/*
