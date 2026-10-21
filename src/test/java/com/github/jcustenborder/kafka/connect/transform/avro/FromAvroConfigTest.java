package com.github.jcustenborder.kafka.connect.transform.avro;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class FromAvroConfigTest {
  @TempDir
  Path temporary;

  @Test
  void requiresExactlyOneSchemaSource() {
    for (Map<String, String> settings : List.of(Map.<String, String>of(),
        Map.of("schema.content", " ", "schema.path", " "),
        Map.of("schema.content", "\"long\"", "schema.path", "file:/schema.avsc"))) {
      assertThrows(ConfigException.class, () -> new FromAvroConfig(settings));
    }
  }

  @Test
  void parsesInlineSchemaAndFileUrl() throws Exception {
    assertEquals(org.apache.avro.Schema.Type.LONG,
        new FromAvroConfig(Map.of("schema.content", "\"long\"")).schema.getType());
    Path schema = Files.writeString(temporary.resolve("schema.avsc"), "\"string\"");
    assertEquals(org.apache.avro.Schema.Type.STRING,
        new FromAvroConfig(Map.of("schema.path", schema.toUri().toString())).schema.getType());
  }

  @Test
  void rejectsInvalidOrUnreadableSchemasWithCause() {
    for (Map<String, String> settings : List.of(Map.of("schema.content", "not-avro"),
        Map.of("schema.path", temporary.resolve("missing.avsc").toUri().toString()),
        Map.of("schema.path", "relative.avsc"), Map.of("schema.path", "ftp://localhost/schema.avsc"))) {
      ConfigException error = assertThrows(ConfigException.class, () -> new FromAvroConfig(settings));
      assertNotNull(error.getCause());
    }
  }

  @Test
  void rejectsExplicitNullSettingsAsConfigurationErrors() {
    for (String setting : List.of("schema.content", "schema.path")) {
      Map<String, Object> config = new HashMap<>(Map.of("schema.content", "\"long\""));
      config.put(setting, null);
      assertThrows(ConfigException.class, () -> new FromAvroConfig(config));
    }
  }

  @Test
  void loadsHttpSchemaAndFailsOnStalledResponse() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    CountDownLatch release = new CountDownLatch(1);
    server.createContext("/schema", exchange -> {
      byte[] content = "\"long\"".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, content.length);
      try (var body = exchange.getResponseBody()) {
        body.write(content);
      }
    });
    server.createContext("/stalled", exchange -> {
      exchange.sendResponseHeaders(200, 100);
      exchange.getResponseBody().write('{');
      exchange.getResponseBody().flush();
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        exchange.close();
      }
    });
    server.start();
    String base = "http://127.0.0.1:" + server.getAddress().getPort();
    try {
      assertEquals(org.apache.avro.Schema.Type.LONG,
          new FromAvroConfig(Map.of("schema.path", base + "/schema")).schema.getType());
      assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
        ConfigException error = assertThrows(ConfigException.class,
            () -> new FromAvroConfig(Map.of("schema.path", base + "/stalled")));
        assertInstanceOf(java.net.SocketTimeoutException.class, error.getCause());
      });
    } finally {
      release.countDown();
      server.stop(0);
    }
  }
}
