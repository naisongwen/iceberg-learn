package org.learn.iceberg.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;

import java.io.File;

public class FlinkIcebergExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        File warehouse = new File("warehouse");
//        try {
//            if (warehouse.exists())
//                FileUtils.deleteDirectory(warehouse);
//        } catch (Exception e) {
//        }
//        warehouse.mkdirs();
        String warehouseLocation = warehouse.getAbsolutePath();
        String createCartalogSql="CREATE CATALOG hadoop_catalog WITH (" +
                "'type'='iceberg',"+
                "'catalog-type'='hadoop'," +
                String.format("'warehouse'='%s',",warehouseLocation)+
                "'property-version'='1'"+
                ")";
        System.out.println(createCartalogSql);
        tableEnvironment.executeSql(createCartalogSql);
        tableEnvironment.executeSql("USE catalog hadoop_catalog");
        tableEnvironment.executeSql("CREATE DATABASE if not EXISTS iceberg_db");
        tableEnvironment.executeSql("USE iceberg_db");
        tableEnvironment.executeSql("CREATE TABLE if not EXISTS  `hadoop_catalog`.`iceberg_db`.`sample` (" +
                "id BIGINT COMMENT 'unique id'," +
                "data STRING" +
                ")");
        tableEnvironment.executeSql("CREATE TABLE if not exists`db`.`tbl1` (" +
                "id BIGINT COMMENT 'unique id'," +
                "data STRING" +
                ")");
        String currCatalog=tableEnvironment.getCurrentCatalog();
        String currDB=tableEnvironment.getCurrentDatabase();
        String [] tables=tableEnvironment.listTables();
        TableLoader tableLoader = TableLoader.fromHadoopTable(warehouseLocation+"/iceberg_db/sample");
        tableLoader.open();
        Table table=tableLoader.loadTable();
        System.out.println(tables);
    }
}
