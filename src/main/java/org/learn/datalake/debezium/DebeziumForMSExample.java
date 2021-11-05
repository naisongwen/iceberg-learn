package org.learn.datalake.debezium;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.util.ExceptionUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.learn.datalake.common.DBConfig;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.debezium.data.Envelope.FieldName.*;
import static java.util.stream.Collectors.toMap;

@Slf4j
public class DebeziumForMSExample {
    Configuration connectorConfiguration() {
        return Configuration.create()
                .with("connector.class", SqlServerConnector.class.getName())
                .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("offset.storage.file.filename", "")
                .with("offset.flush.interval.ms", 15000)
                .with("name", "mysql-connector")
                //是否包含数据库表结构层面的变更，建议使用默认值true
                .with("include.schema.changes", "false")
                // unique within all processes that make up the MySQL server group and is any integer between 1 and 232-1
                //.with("database.server.id", "85744")
                //MySQL 服务器或集群的逻辑名称
                .with("database.server.name", "mssql_binlog_source")
                .with("database.hostname", DBConfig.MSTestDB.host)
                .with("database.port", DBConfig.MSTestDB.port)
                .with("database.user", DBConfig.MSTestDB.userName)
                .with("database.password", DBConfig.MSTestDB.passWd)
                .with("database.dbname", DBConfig.MSTestDB.dbName)
                .with("database.include.list", DBConfig.MSTestDB.dbName)
                .with("table.include.list", DBConfig.MSTestDB.tableName)
                //历史变更记录
                .with("database.history", "io.debezium.relational.history.MemoryDatabaseHistory")
                //历史变更记录存储位置
                .with("database.history.file.filename", "")
                .build();
    }

    void handleEvent(SourceRecord sourceRecord) {
        Struct sourceRecordValue = (Struct) sourceRecord.value();
        if (sourceRecordValue != null) {
            // 判断操作的类型 过滤掉读 只处理增删改   这个其实可以在配置中设置
            Envelope.Operation operation = Envelope.Operation.forCode((String) sourceRecordValue.get(OPERATION));
            if (operation != Envelope.Operation.READ) {
                // 获取增删改对应的结构体数据
                Struct struct = (Struct) sourceRecordValue.get(operation == Envelope.Operation.DELETE ? BEFORE : AFTER);
                // 将变更的行封装为Map
                Map<String, Object> payload = struct.schema().fields().stream()
                        .map(Field::name)
                        .filter(fieldName -> struct.get(fieldName) != null)
                        .map(fieldName -> Pair.of(fieldName, struct.get(fieldName)))
                        .collect(toMap(Pair::getKey, Pair::getValue));

                System.out.println(payload);
            }
        }
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

    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10L, TimeUnit.SECONDS)) {
                log.info("Waiting another 10 seconds for the embedded engine to shut down");
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    /**
     * The Debezium engine which needs to be loaded with the configurations, Started and Stopped - for the
     * CDC to work.
     */

    Configuration conf = connectorConfiguration();

    public void start0() {
        EmbeddedEngine engine = EmbeddedEngine
                .create()
                .using(conf)
                .notifying(this::handleEvent).build();
        /**
         * Single thread pool which will run the Debezium engine asynchronously.
         */
        Executor executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
    }

    public void start() {
        // Engine is stopped when the main code is finished when warped by try()
        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(conf.asProperties())
                .notifying(record -> {
                    System.out.println(record);
                }).build();
        {
            // Run the engine asynchronously ...
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);

            shutdownHook(engine);

            awaitTermination(executor);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        DebeziumForMSExample debeziumExample = new DebeziumForMSExample();
        debeziumExample.start();
    }

}
