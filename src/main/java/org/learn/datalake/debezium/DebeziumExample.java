package org.learn.datalake.debezium;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.learn.datalake.common.MysqlConfig;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.debezium.data.Envelope.FieldName.*;
import static java.util.stream.Collectors.toMap;

public class DebeziumExample {
    Configuration connectorConfiguration() {
        return io.debezium.config.Configuration.create()
                .with("connector.class", MySqlConnector.class.getName())
                .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("offset.storage.file.filename", "")
                .with("offset.flush.interval.ms", 15000)
                .with("name", "mysql-connector")
                //是否包含数据库表结构层面的变更，建议使用默认值true
                .with("include.schema.changes", "false")
                // unique within all processes that make up the MySQL server group and is any integer between 1 and 232-1
                //.with("database.server.id", "85744")
                //MySQL 服务器或集群的逻辑名称
                .with("database.server.name", "mysql_binlog_source")
                .with("database.hostname", MysqlConfig.host)
                .with("database.port", MysqlConfig.port)
                .with("database.user", MysqlConfig.userName)
                .with("database.password", MysqlConfig.passWd)
                .with("database.dbname", MysqlConfig.dbName)
                .with("database.include.list", MysqlConfig.dbName)
                //.with("table.include.list", MysqlConfig.tableName)
                //历史变更记录
                .with("database.history", "io.org.learn.datalake.debezium.relational.history.MemoryDatabaseHistory")
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
                .notifying(new JsonChangeConsumer()).build();
//                .notifying(record -> {
//                    System.out.println(record);
//                }).build();
        {
            // Run the engine asynchronously ...
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);
        }
    }

    public static void main(String args[]) throws InterruptedException, IOException {
        DebeziumExample debeziumExample = new DebeziumExample();
        debeziumExample.start();
    }

}
