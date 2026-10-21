/**
 * Copyright © 2026 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.transform.avro;

import org.apache.avro.Schema;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.Set;

class FromAvroConfig extends AbstractConfig {
  static final String SCHEMA_PATH_CONFIG = "schema.path";
  static final String SCHEMA_CONTENT_CONFIG = "schema.content";
  final Schema schema;

  FromAvroConfig(Map<String, ?> settings) {
    super(config(), settings);
    String path = getString(SCHEMA_PATH_CONFIG);
    String content = getString(SCHEMA_CONTENT_CONFIG);
    if (path == null || content == null) {
      throw new ConfigException("schema.path and schema.content must not be null");
    }
    path = path.trim();
    if (path.isEmpty() == content.isBlank()) {
      throw new ConfigException("Exactly one of schema.path or schema.content must be configured");
    }
    try {
      if (path.isEmpty()) {
        this.schema = new Schema.Parser().parse(content);
      } else {
        URL url = URI.create(path).toURL();
        if (!Set.of("file", "http", "https").contains(url.getProtocol())) {
          throw new IllegalArgumentException("schema.path must use file, http or https");
        }
        URLConnection connection = url.openConnection();
        connection.setConnectTimeout(10_000);
        connection.setReadTimeout(10_000);
        try (InputStream input = connection.getInputStream()) {
          this.schema = new Schema.Parser().parse(input);
        }
      }
    } catch (IOException | RuntimeException e) {
      ConfigException error = new ConfigException("Could not load Avro writer schema from "
          + (path.isEmpty() ? SCHEMA_CONTENT_CONFIG : SCHEMA_PATH_CONFIG + ": " + path));
      error.initCause(e);
      throw error;
    }
  }

  static ConfigDef config() {
    return new ConfigDef()
        .define(SCHEMA_PATH_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
            "Writer schema URL (file, http, https). Configure exactly one of schema.path or schema.content.")
        .define(SCHEMA_CONTENT_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
            "Inline Avro writer schema JSON. Configure exactly one of schema.path or schema.content.");
  }
}
