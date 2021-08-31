package org.learn.datalake.debezium;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.spi.OffsetCommitPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.debezium.data.Envelope.FieldName.OPERATION;

@Slf4j
public class DebeziumEmbeddedRunner {
  static final Logger LOG = LoggerFactory.getLogger(DebeziumEmbeddedRunner.class);

  Configuration configuration;

  private JsonConverter keyConverter;

  private JsonConverter valueConverter;

  public void run(String... args) {
    DebeziumChangeConsumer debeziumConsumer = new DebeziumChangeConsumer();
    // create the engine with this configuration ...
    DebeziumEngine<?> engine =
        DebeziumEngine.create(Connect.class)
            .using(DebeziumConfig.connectorConfiguration().asProperties())
            .notifying(debeziumConsumer)
            .using(OffsetCommitPolicy.always())
            .using(
                (success, message, error) -> {
                  if (!success && error != null) {
                    this.reportError(error);
                  }
                })
            .build();

    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.execute(engine);
  }

  private Envelope.Operation getOperation(Struct payload) {
    return Envelope.Operation.forCode((String) payload.get(OPERATION));
  }

  private Struct getRecordStructValue(Struct payload, String key) {
    Struct struct = (Struct) payload.get(key);
    return struct;
  }

  private String getDatabaseName(Struct payload) {
    String table =
        Optional.ofNullable(getRecordStructValue(payload, "source"))
            .map(s -> s.getString("db"))
            .orElse(null);
    return table;
  }

  private String getTableName(Struct payload) {
    String table =
        Optional.ofNullable(getRecordStructValue(payload, "source"))
            .map(s -> s.getString("table"))
            .orElse(null);
    return table;
  }

  /** For every record this method will be invoked. */
  private void handleRecord(SourceRecord record) {
    logRecord(record);

    Struct payload = (Struct) record.value();
    if (Objects.isNull(payload)) {
      return;
    }
    String table = getTableName(payload);

    // 处理数据DML
    Envelope.Operation operation = getOperation(payload);
    if (Objects.nonNull(operation)) {
      Struct key = (Struct) record.key();
      handleDML();
      return;
    }

    // 处理结构DDL
    String ddl = null; // getDDL(payload);
    if (StringUtils.isNotBlank(ddl)) {
      handleDDL(ddl);
    }
  }

  //    private String getDDL(Struct payload) {
  //        String ddl = DebeziumRecordUtils.getDDL(payload);
  //        if (StringUtils.isBlank(ddl)) {
  //            return null;
  //        }
  //        String db = getDatabaseName(payload);
  //        if (StringUtils.isBlank(db)) {
  //            db = embeddedConfig.getString(MySqlConnectorConfig.DATABASE_WHITELIST);
  //        }
  //        ddl = ddl.replace(db + ".", "");
  //        ddl = ddl.replace("`" + db + "`.", "");
  //        return ddl;
  //    }

  private void handleDML() {}

  private void handleDDL(String ddl) {
    log.info("ddl语句 : {}", ddl);
    try {
      // jdbcTemplate.execute(ddl);
    } catch (Exception e) {
      log.error("数据库操作DDL语句失败，", e);
    }
  }

  private void logRecord(SourceRecord record) {
    final byte[] payload =
        valueConverter.fromConnectData("dummy", record.valueSchema(), record.value());
    final byte[] key = keyConverter.fromConnectData("dummy", record.keySchema(), record.key());
    log.info("Publishing Topic --> {}", record.topic());
    log.info("Key --> {}", new String(key));
    log.info("Payload --> {}", new String(payload));
  }

  private void shutdownHook(DebeziumEngine<?> engine) {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("Requesting embedded engine to shut down");
                  try {
                    engine.close();
                  } catch (IOException e) {
                    ExceptionUtils.rethrow(e);
                  }
                }));
  }

  private void reportError(Throwable error) {
    LOG.error("Reporting error:", error);
  }

  public static void main(String[] args) {
    DebeziumEmbeddedRunner debeziumEmbeddedRunner = new DebeziumEmbeddedRunner();
    debeziumEmbeddedRunner.run();
  }
}
