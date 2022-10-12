package org.learn.datalake.spark.sql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

public class SparkSqlTest {

    @Test
    public void testInsert() {
        String hmsUri = "thrift://10.201.0.212:39083";
        String warehouse="/Users/deepexi/workplace/hive-learn/warehouse/";
        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
                .config("spark.hadoop." + METASTOREURIS.varname, hmsUri)
                .config("spark.hadoop.hive.metastore.warehouse.dir", warehouse)
                .config("spark.sql.warehouse.dir", warehouse)
                .enableHiveSupport()
                .getOrCreate();

        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.warehouse.dir",warehouse);
        HiveCatalog catalog = (HiveCatalog)
                CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);
        String catalogName = "test_hive_catalog";
        sparkSession.conf().set("spark.sql.catalog." + catalogName, SparkCatalog.class.getName());
        Map<String, String> config =ImmutableMap.of(
                "type", "hive",
                "warehouse", warehouse,
                "default-namespace", "default"
        );
        config.forEach((key, value) -> sparkSession.conf().set("spark.sql.catalog." + catalogName + "." + key, value));
//        catalog.createNamespace(Namespace.of("default"));
        String query = "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg";
        String tableName = String.format("%s.default.test_tbl_7" +
                "",catalogName);
        sparkSession.sql(String.format(query, tableName));
        query = "insert into  %s values(1,'aaa')";
        sparkSession.sql(String.format(query, tableName));
        query = "select * from %s";
        List<Row> rows = sparkSession.sql(String.format(query, tableName)).collectAsList();
        System.out.println(rows);
    }
}
