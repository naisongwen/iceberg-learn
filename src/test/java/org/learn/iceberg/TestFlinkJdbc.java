package org.learn.iceberg;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

public class TestFlinkJdbc {

    @Test
    public void testMysql() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions
                .JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://10.201.0.214:3306")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                .build();

        String catalogName = "jdbcCatalog";
        JdbcCatalog catalog = new JdbcCatalog(Thread.currentThread().getContextClassLoader(), catalogName, "test", connectionOptions.getUsername().get(), connectionOptions.getPassword().get(), connectionOptions.getDbURL());

        tEnv.registerCatalog(catalogName, catalog);
        tEnv.useCatalog(catalogName);

        String createTableSql = "CREATE TABLE %s (\n" +
                "  id BIGINT,\n" +
                "  name STRING,\n" +
                "  age INT,\n" +
                "  status BOOLEAN,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'table-name' = '%s'\n" +
                ");";
        String tableName = "test_flink_jdbc_tbl";
        tEnv.executeSql(String.format(createTableSql, tableName,tableName));
        tEnv.executeSql("insert into " + tableName + " values(1,'xxx',18,true)");
    }
}
