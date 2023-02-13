//package org.learn.datalake.common;
//
//import io.debezium.config.Configuration;
//import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
//import io.debezium.jdbc.JdbcConfiguration;
//import io.debezium.relational.RelationalDatabaseConnectorConfig;
//import io.debezium.relational.history.FileDatabaseHistory;
//
//public class DBTools {
//    public static Configuration.Builder jdbcConfig(String database,String host,String user,String password,int port) {
//        JdbcConfiguration jdbcConfiguration = JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
//                .withDefault(JdbcConfiguration.DATABASE, database)
//                .withDefault(JdbcConfiguration.HOSTNAME, host)
//                .withDefault(JdbcConfiguration.PORT, port)
//                .withDefault(JdbcConfiguration.USER, user)
//                .withDefault(JdbcConfiguration.PASSWORD,password)
//                .build();
//        Configuration.Builder builder = Configuration.create();
//
//        jdbcConfiguration.forEach(
//                (field, value) -> builder.with(SqlServerConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));
//
//        return builder.with(RelationalDatabaseConnectorConfig.SERVER_NAME, "server1")
//                .with(SqlServerConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
////                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
//                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
//    }
//}
