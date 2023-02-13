package org.learn.iceberg.debezium;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.json.JsonConverter;
import org.learn.iceberg.common.DBConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class DebeziumConfig {
    public static Configuration connectorConfiguration() {
        return Configuration.create()
                .with("connector.class", MySqlConnector.class.getCanonicalName())
                .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("offset.storage.file.filename", "")
                .with("offset.flush.interval.ms", 1000)
                .with("name", "mysql-connector")
                //是否包含数据库表结构层面的变更，建议使用默认值true
                .with("include.schema.changes", "false")
                // unique within all processes that make up the MySQL server group and is any integer between 1 and 232-1
                //.with("database.server.id", "85744")
                //MySQL 服务器或集群的逻辑名称
                .with("database.server.name", "mysql_binlog_source")
                .with("database.hostname", DBConfig.MYSQLJKDB.host)
                .with("database.port", DBConfig.MYSQLJKDB.port)
                .with("database.user", DBConfig.MYSQLJKDB.userName)
                .with("database.password", DBConfig.MYSQLJKDB.passWd)
                .with("database.dbname", DBConfig.MYSQLJKDB.dbName)
                .with("database.whitelist", DBConfig.MYSQLJKDB.dbName)
                //full table name
                .with("table.whitelist", String.format("%s.%s", DBConfig.MYSQLJKDB.dbName,DBConfig.MYSQLJKDB.tableName))
                .with("database.history.skip.unparseable.ddl", String.valueOf(true))
                //历史变更记录
                .with("database.history", "io.debezium.relational.history.MemoryDatabaseHistory")
                //历史变更记录存储位置
                .with("database.history.file.filename", "")
                .build();
    }

    public Properties embeddedProperties() {
        Properties propConfig = new Properties();
        try(InputStream propsInputStream = getClass().getClassLoader().getResourceAsStream("config/config.properties")) {
            propConfig.load(propsInputStream);
        } catch (IOException e) {
            log.error("Couldn't load properties", e);
        }
        return propConfig;
    }

//    @Bean
//    public io.debezium.config.Configuration embeddedConfig(Properties embeddedProperties) {
//        return io.debezium.config.Configuration.from(embeddedProperties);
//    }

    public static JsonConverter keyConverter(Configuration embeddedConfig) {
        JsonConverter converter = new JsonConverter();
        converter.configure(embeddedConfig.asMap(), true);
        return converter;
    }

    public static JsonConverter valueConverter(Configuration embeddedConfig) {
        JsonConverter converter = new JsonConverter();
        converter.configure(embeddedConfig.asMap(), false);
        return converter;
    }
}
