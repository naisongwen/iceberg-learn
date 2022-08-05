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
        String hmsUri = "thrift://localhost:9083";
        String warehouse = "s3a://faas-ethan/warehouse/";
        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
//                .config("spark.hadoop." + METASTOREURIS.varname, hmsUri)
                .config("spark.hadoop.hive.metastore.warehouse.dir", warehouse)
                .config("spark.hadoop.fs.s3a.access.key", "admin1234")
                .config("spark.hadoop.fs.s3a.secret.key", "admin1234")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                .config("spark.hadoop.hive.metastore.schema.verification", "false")
                .config("spark.sql.warehouse.dir", warehouse)
                .enableHiveSupport()
                .getOrCreate();

        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.warehouse.dir", warehouse);
        HiveCatalog catalog = (HiveCatalog)
                CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);
        String catalogName = "test_hive_catalog";
        String table="test_tbl_7";
        sparkSession.conf().set("spark.sql.catalog." + catalogName, SparkCatalog.class.getName());
        Map<String, String> config = ImmutableMap.of(
                "type", "hive",
                "uri", "thrift://10.201.0.202:49157",
                "warehouse", warehouse,
                "default-namespace", "default"
        );
        config.forEach((key, value) -> sparkSession.conf().set("spark.sql.catalog." + catalogName + "." + key, value));
//        catalog.createNamespace(Namespace.of("default"));
        String tableName = String.format("%s.default.%s", catalogName,table);
        String query =String.format("CREATE TABLE %s (id bigint NOT NULL, data string)\n" +
                " USING iceberg\n" +
                "TBLPROPERTIES ('iceberg.catalog'='%s')\n" +
                "location '%s/default/%s';",tableName,catalogName,warehouse,tableName);
        sparkSession.sql(query);
        query = String.format("insert into  %s values(1,'aaa')",tableName);
        sparkSession.sql(query);
        query = String.format("select * from %s",tableName);
        List<Row> rows = sparkSession.sql(query).collectAsList();
        System.out.println(rows);
    }
}
