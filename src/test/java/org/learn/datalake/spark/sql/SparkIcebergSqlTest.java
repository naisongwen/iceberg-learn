package org.learn.datalake.spark.sql;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SparkIcebergSqlTest {//extends SparkTestBase {
    protected static String hiveMetastoreURI="thrift://10.201.0.212:39083";
    protected static String warehouse = "s3a://faas-ethan/warehouse/";
    protected static String defaultCatalogName = "aaaa_mapping_1982";
    protected static String hmsUri = "thrift://10.201.0.212:39083";
    protected static String table="test_tbl_11";

    @Test
    public void testIcebergCatalogQuery() {
        String warehouse = "s3a://faas-ethan/warehouse/";
        SparkSession.Builder builder = SparkSession.builder()
                .master("local[2]")
                .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
                .config("spark.hadoop.hive.metastore.warehouse.dir", warehouse)
                .config("spark.hadoop.fs.s3a.access.key", "admin1234")
                .config("spark.hadoop.fs.s3a.secret.key", "admin1234")
                .config("spark.hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")
                .config("fs.s3a.impl.disable.cache", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                .config("spark.hadoop.hive.metastore.schema.verification", "false")
                .config("spark.sql.warehouse.dir", warehouse);

        String catalogName = "test_hive_catalog";
        builder.config("spark.sql.catalog." + catalogName, SparkCatalog.class.getName());
        Map<String, String> config = ImmutableMap.of(
                "type", "hive",
                "uri", hmsUri,
                "warehouse", warehouse
        );
        config.forEach((key, value) -> builder.config("spark.sql.catalog." + catalogName + "." + key, value));
        SparkSession sparkSession = builder.enableHiveSupport().getOrCreate();
        //        catalog.createNamespace(Namespace.of("default"));
        String tableName = String.format("%s.default.%s", catalogName, table);
        String query = String.format("CREATE TABLE %s (id bigint NOT NULL, data string)\n" +
                " USING iceberg\n" +
                "TBLPROPERTIES ('iceberg.catalog'='%s')\n" +
                "location '%s/default/%s';", tableName, catalogName, warehouse, tableName);
        sparkSession.sql(query);
        query = String.format("insert into  %s values(1,'aaa')", tableName);
        sparkSession.sql(query);
        query = String.format("select * from %s", tableName);
        List<Row> rows = sparkSession.sql(query).collectAsList();
        System.out.println(rows);
        sparkSession.sql(String.format("drop table %s", tableName));
    }

    @Test
    public void testIcebergSessionCatalogQuery() {
        SparkSession.Builder builder = SparkSession.builder()
                .master("local[2]")
                .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
//                .config("spark.hadoop." + METASTOREURIS.varname, hmsUri)
                .config("spark.hadoop.hive.metastore.warehouse.dir", warehouse)
//                .config("spark.sql.catalog.test_hive_catalog.hadoop.fs.s3a.access.key", "admin1234")
//                .config("spark.sql.catalog.test_hive_catalog.hadoop.fs.s3a.secret.key", "admin1234")
//                .config("spark.sql.catalog.test_hive_catalog.hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")

                .config("spark.hadoop.fs.s3a.access.key", "admin1234")
                .config("spark.hadoop.fs.s3a.secret.key", "admin1234")
                .config("spark.hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")
                .config("fs.s3a.impl.disable.cache", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                .config("spark.hadoop.hive.metastore.schema.verification", "false")
                .config("spark.sql.warehouse.dir", warehouse);

        String catalogName = "spark_catalog";
        builder.config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName());
        builder.config("spark.sql.catalog.spark_catalog", SparkSessionCatalog.class.getName());
        Map<String, String> config = ImmutableMap.of(
                "type", "hive",
                "uri", hmsUri,
                "warehouse", warehouse
        );
        config.forEach((key, value) -> builder.config("spark.sql.catalog.spark_catalog." + key, value));
        SparkSession sparkSession = builder.enableHiveSupport().getOrCreate();
        //        catalog.createNamespace(Namespace.of("default"));
        String tableName = String.format("%s.default.%s", catalogName, table);
        String query = String.format("CREATE TABLE %s (id bigint NOT NULL, data string)\n" +
                " USING iceberg\n" +
                "TBLPROPERTIES ('iceberg.catalog'='%s')\n" +
                "location '%s/default/%s';", tableName, catalogName, warehouse, tableName);
        sparkSession.sql(query);
        query = String.format("insert into  %s values(1,'aaa')", tableName);
        sparkSession.sql(query);
        query = String.format("select * from %s", tableName);
        List<Row> rows = sparkSession.sql(query).collectAsList();
        System.out.println(rows);
        sparkSession.sql(String.format("drop table %s", tableName));
    }
}
