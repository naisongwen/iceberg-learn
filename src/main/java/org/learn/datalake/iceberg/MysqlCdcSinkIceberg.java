package org.learn.datalake.iceberg;

import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED;

public class MysqlCdcSinkIceberg{

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //1.12默认时间属性为 EventTime 无需设置
        env.setParallelism(1);

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        tableEnv.getConfig().getConfiguration().setString(TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key(),"true");

        env.enableCheckpointing(10000);

        String cdcCreateTableSQL =
                "CREATE TABLE prod_cdc_orders_source(\n" +
                        "     o_orderkey INT, \n" +
                        "     o_clerk STRING, \n" +
                        "     PRIMARY KEY (o_orderkey) NOT ENFORCED \n"+
                        ") WITH ( \n" +
                        "     'connector' = 'mysql-cdc', \n" +
                        "     'hostname' = '10.201.0.213', \n" +
                        "     'port' = '3306', \n" +
                        "     'username' = 'root', \n" +
                        "     'password' = 'root', \n" +
                        "     'database-name' = 'case_test_ads_db', \n" +
                        "     'table-name' = 'connector_cdc_source'\n" +
                        ")";
        tableEnv.executeSql(cdcCreateTableSQL);
        File warehouse = new File("warehouse/test_cdc_sink");
        if (warehouse.exists()) {
            FileUtils.cleanDirectory(warehouse);
        }
        //iceberg
        tableEnv.executeSql("CREATE CATALOG defaultIcebergCatalog " +
                "WITH (" +
                " 'type'='iceberg', " +
                " 'catalog-type'='hadoop', " +
                String.format(" 'warehouse'='%s')", warehouse));
                //" 'warehouse'='hdfs://10.201.0.212:8020/khnib')");

        tableEnv.executeSql("USE CATALOG defaultIcebergCatalog");
        tableEnv.executeSql("CREATE DATABASE defaultIcebergDB");
        tableEnv.executeSql("USE defaultIcebergDB");
       tableEnv.executeSql("CREATE TABLE if not exists testIcebergTable (" +
                "     o_orderkey INT, \n" +
                "     o_userName STRING, \n" +
                "     PRIMARY KEY (o_orderkey) NOT ENFORCED \n"+
                ") WITH (" +
               "    'write.format.default'='parquet'," +
               "     'format-version'='2')");

        Table table=tableEnv.sqlQuery( "select * from `default_catalog`.`default_database`.prod_cdc_orders_source");
        table.printSchema();
        //write
        tableEnv.executeSql("INSERT INTO `defaultIcebergCatalog`.`defaultIcebergDB`.`testIcebergTable` " +
                "select * from `default_catalog`.`default_database`.prod_cdc_orders_source");

//        //iceberg result read
//        tableEnv.executeSql("select count(orderId) from " +
//                " `defaultCatalog`.`defaultkhnDB`.`t_kafka_only_insert_smallsize_sink` /*+ OPTIONS('streaming'='true')*/ ").print();

//        tableEnv.executeSql("select sum(o_totalprice) from " +
//                " `defaultCatalog`.`defaultkhnDB`.`t_kafka_only_insert_sink` /*+ OPTIONS('streaming'='true')*/ ").print();

    }

}
