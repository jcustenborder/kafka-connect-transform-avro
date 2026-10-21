package com.github.jcustenborder.kafka.connect.transform.avro;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PackagedRuntimeIT {
  @TempDir
  Path temporary;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void packagedPluginConvertsWithProvidedConverter(boolean inline) throws Exception {
    Path bundle = Path.of(System.getProperty("plugin.zip"));
    Path unpacked = temporary.resolve("plugin");
    try (ZipFile zip = new ZipFile(bundle.toFile())) {
      for (ZipEntry entry : java.util.Collections.list(zip.entries())) {
        Path target = unpacked.resolve(entry.getName()).normalize();
        if (!target.startsWith(unpacked)) {
          throw new IOException("Invalid plugin archive entry " + entry.getName());
        }
        if (!entry.isDirectory()) {
          Files.createDirectories(target.getParent());
          try (InputStream input = zip.getInputStream(entry)) {
            Files.copy(input, target);
          }
        }
      }
    }
    String archiveName = bundle.getFileName().toString();
    Path libraries = unpacked.resolve(archiveName.substring(0, archiveName.length() - 4)).resolve("lib");
    Path jar = libraries.resolve(Path.of(System.getProperty("plugin.jar")).getFileName());
    // Keep worker/converter dependencies, but require production classes and connect-utils from the ZIP.
    String provided = Arrays.stream(System.getProperty("surefire.test.class.path").split(File.pathSeparator))
        .filter(path -> !Path.of(path).toAbsolutePath().normalize().equals(Path.of("target/classes").toAbsolutePath()))
        .filter(path -> !path.contains("/com/github/jcustenborder/"))
        .collect(Collectors.joining(File.pathSeparator));
    String classpath = libraries.resolve("*") + File.pathSeparator + provided;
    Path output = temporary.resolve("runtime.log");
    Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-cp", classpath, Probe.class.getName(), jar.toString(), Boolean.toString(inline))
        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    if (!process.waitFor(60, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      throw new AssertionError("Packaged conversion timed out: " + Files.readString(output));
    }
    assertEquals(0, process.exitValue(), Files.readString(output));
  }

  public static class Probe {
    public static void main(String[] args) throws Exception {
      Path actualJar = Path.of(FromAvro.class.getProtectionDomain().getCodeSource().getLocation().toURI());
      if (!Files.isSameFile(actualJar, Path.of(args[0]))) {
        throw new AssertionError("Expected packaged plugin " + args[0] + " but loaded " + actualJar);
      }
      Set<Class<?>> providers = new HashSet<>();
      for (Transformation<?> provider : ServiceLoader.load(Transformation.class)) {
        providers.add(provider.getClass());
        provider.close();
      }
      if (!providers.containsAll(Set.of(FromAvro.Key.class, FromAvro.Value.class))) {
        throw new AssertionError("Missing transformation providers: " + providers);
      }
      String schema = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
      Path file = Files.writeString(Path.of(args[0]).getParent().resolve("event.avsc"), schema);
      Map<String, String> settings = Boolean.parseBoolean(args[1])
          ? Map.of("schema.content", schema) : Map.of("schema.path", file.toUri().toString());
      for (boolean key : List.of(false, true)) {
        try (FromAvro<SinkRecord> transform = key ? new FromAvro.Key<>() : new FromAvro.Value<>()) {
          transform.configure(settings);
          SinkRecord result = transform.apply(new SinkRecord("events", 0,
              key ? Schema.BYTES_SCHEMA : null, key ? new byte[]{84} : null,
              key ? null : Schema.BYTES_SCHEMA, key ? null : new byte[]{84}, 0L));
          Schema outputSchema = key ? result.keySchema() : result.valueSchema();
          Object value = key ? result.key() : result.value();
          ConnectSchema.validateValue(outputSchema, value);
          if (!"Event".equals(outputSchema.name()) || !Long.valueOf(42).equals(((Struct) value).getInt64("id"))) {
            throw new AssertionError("Incorrect structured Avro output: " + value);
          }
        }
      }
    }
  }
}
