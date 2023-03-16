package org.learn.datalake.spark.catalog;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

public class IcebergSparkCatalogTest {//extends SparkTestBase {

    protected static final DataType STRING_MAP = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);
    private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
            ProcedureParameter.required("table", DataTypes.StringType),
            ProcedureParameter.required("source_table", DataTypes.StringType),
            ProcedureParameter.optional("partition_filter", STRING_MAP)
    };

    protected static String hiveMetastoreURI = "thrift://10.201.0.212:39083";
    protected static String warehouse = "s3a://faas-ethan/warehouse/";
    protected static String defaultCatalogName = "aaaa_mapping_1982";

    String anotherCatalogMappingName = "another_catalog";
    protected static String defaultDatabase = "dlink_default";
    protected static String table = "test_spark_tbl_2";

    protected static Spark3Util.CatalogAndIdentifier toCatalogAndIdentifier(String identifierAsString, String argName,
                                                                            CatalogPlugin catalog, SparkSession spk) {
        Preconditions.checkArgument(identifierAsString != null && !identifierAsString.isEmpty(),
                "Cannot handle an empty identifier for argument %s", argName);

        return Spark3Util.catalogAndIdentifier("identifier for arg " + argName, spk, identifierAsString, catalog);
    }

    @Test
    public void testSparkHiveCatalog() {
        SparkConf sparkConf = new SparkConf()
                .set("spark.default.parallelism", "1")
                .set(METASTOREURIS.varname, hiveMetastoreURI)
                .set("spark.sql.warehouse.dir", "warehouse")
                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        sparkSession.sql("show databases").show();
    }

    @Test
    public void testSparkOtherCatalog() {
        SparkConf sparkConf = new SparkConf()
                .set("spark.default.parallelism", "1")
//                MetastoreConf available in HMS 3.x
                .set(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname(), defaultCatalogName)
                .set(METASTOREURIS.varname, hiveMetastoreURI)
//                spark.hadoop.+ hadoop configuration
//                .set("spark.hadoop." + METASTOREURIS.varname, hiveMetastoreURI)

                .set("spark.sql.warehouse.dir", "/tmp/")
//                HIVE_METASTORE_VERSION defined in HiveUtils.scala of Spark-hive jars
//                .set("spark.sql.hive.metastore.version", "3.1.2")
//                .set("spark.sql.hive.metastore.jars", String.format("%s/lib/*", hiveHome))

                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        sparkSession.sql("show databases").show();
    }

    @Test
    public void testIcebergHiveCatalog() {
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", hiveMetastoreURI);
        Configuration conf = new Configuration();
//        metastore.catalog.default available since HMS 3.x
        conf.set("metastore.catalog.default", defaultCatalogName);
        HiveCatalog catalog = new HiveCatalog();
        catalog.setConf(conf);
        catalog.initialize(defaultCatalogName, properties);
        System.out.println("==========> list iceberg catalog databases");
        catalog.listNamespaces().forEach(System.out::println);
    }

    @Test
    public void testIcebergSparkSessionCatalog() {
        SparkConf sparkConf = new SparkConf()
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog.spark_catalog.type", "hive")
                .set("spark.sql.catalog.spark_catalog.default-namespace", defaultDatabase)
                .set("spark.sql.catalog.spark_catalog.uri", hiveMetastoreURI)
                .set("spark.sql.catalog.spark_catalog.warehouse", warehouse)
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.access.key", "admin1234")
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.secret.key", "admin1234")
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")
                .set("spark.sql.catalog.spark_catalog.hadoop.metastore.catalog.default", defaultCatalogName)
                .set("spark.sql.catalog.spark_catalog.hadoop."+METASTOREURIS.varname, hiveMetastoreURI)
                .set(METASTOREURIS.varname, hiveMetastoreURI)
                .set("metastore.catalog.default", defaultCatalogName)
                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .set("spark.default.parallelism", "1")
                .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true") // support read data of timestamp-without-timezone
                .set("spark.sql.warehouse.dir", "warehouse")
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
//        sparkSession.sql("show databases").show();
        String fullTableName = String.format("%s.%s.%s","spark_catalog",defaultDatabase,table);
        sparkSession.sql(String.format("drop table if exists %s",fullTableName));
        sparkSession.sql(String.format("create table if not exists %s(id int) USING iceberg TBLPROPERTIES ('iceberg.catalog'='spark_catalog')",fullTableName));
        sparkSession.sql(String.format("insert into %s values(1)",fullTableName));
        sparkSession.sql(String.format("select * from %s",fullTableName));
        sparkSession.sql(String.format("drop table %s",fullTableName));
    }

    @Test
    public void testIcebergSparkCatalog() {
        SparkConf sparkConf = new SparkConf()
                .set("spark.sql.catalog." + anotherCatalogMappingName, "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".type", "hive")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".default-namespace", "default")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".uri", hiveMetastoreURI)
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".warehouse", warehouse)
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.access.key", "admin1234")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.secret.key", "admin1234")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.metastore.catalog.default", defaultCatalogName)
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop."+METASTOREURIS.varname, hiveMetastoreURI)
//                .set(METASTOREURIS.varname, hiveMetastoreURI)
                .set("metastore.catalog.default", defaultCatalogName)
                .set("spark.default.parallelism", "1")
                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true") // support read data of timestamp-without-timezone
                .set("spark.sql.warehouse.dir", "warehouse")
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
//        sparkSession.sql("show databases").show();
        String fullTableName = String.format("%s.%s.%s",anotherCatalogMappingName,defaultDatabase,table);
        sparkSession.sql(String.format("drop table if exists %s",fullTableName));
        sparkSession.sql(String.format("create table if not exists %s(id int) USING iceberg TBLPROPERTIES ('iceberg.catalog'='spark_catalog')",fullTableName));
        sparkSession.sql(String.format("insert into %s values(1)",fullTableName));
        sparkSession.sql(String.format("select * from %s",fullTableName));
        sparkSession.sql(String.format("drop table %s",fullTableName));
    }

    @Test
    public void testMultipleCatalogs() {
        String anotherHiveMetastoreURI = "thrift://10.201.0.202:30470";

        SparkConf sparkConf = new SparkConf()
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog.spark_catalog.type", "hive")
                .set("spark.sql.catalog.spark_catalog.default-namespace", defaultDatabase)
                .set("spark.sql.catalog.spark_catalog.uri", hiveMetastoreURI)
                .set("spark.sql.catalog.spark_catalog.warehouse", warehouse)
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.access.key", "admin1234")
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.secret.key", "admin1234")
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")
                .set("spark.sql.catalog.spark_catalog.hadoop.metastore.catalog.default", defaultCatalogName)
                .set("spark.default.parallelism", "1")
                .set(METASTOREURIS.varname, hiveMetastoreURI)
                .set("metastore.catalog.default", defaultCatalogName)
                .set("fs.s3a.impl.disable.cache", "true")
                .set("spark.sql.catalog." + anotherCatalogMappingName, "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".type", "hive")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".default-namespace", "default")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".uri", anotherHiveMetastoreURI)
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".warehouse", warehouse)
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.access.key", "deepexi2022")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.secret.key", "deepexi2022")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.endpoint", "http://10.201.0.202:30977")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.metastore.catalog.default", "zzjtest_mapping_517")

                .set("spark.default.parallelism", "24")
                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true") // support read data of timestamp-without-timezone
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        String fullTableName1 = String.format("%s.%s.%s","spark_catalog",defaultDatabase,table);
        String fullTableName2 = String.format("%s.%s.%s",anotherCatalogMappingName,"yy",table);
        sparkSession.sql(String.format("drop table if exists %s",fullTableName1));
        sparkSession.sql(String.format("create table if not exists %s(id int) USING iceberg TBLPROPERTIES ('iceberg.catalog'='spark_catalog')",fullTableName1));
        sparkSession.sql(String.format("insert into %s values(1)",fullTableName1));

        sparkSession.sql(String.format("drop table if exists %s",fullTableName2));
        sparkSession.sql(String.format("create table if not exists %s(id int) USING iceberg TBLPROPERTIES ('iceberg.catalog'='spark_catalog')",fullTableName2));
        sparkSession.sql(String.format("insert into %s values(1)",fullTableName2));
        sparkSession.sql(String.format("select * from %s,%s",fullTableName1,fullTableName2));
//        sparkSession.sql(String.format("drop table %s",fullTableName1));
//        sparkSession.sql(String.format("drop table %s",fullTableName2));
    }

//        SessionCatalog sessionCatalog = sparkSession.sessionState().catalog();
//
//        CatalogPlugin v2SessionCatalog = sparkSession.sessionState().catalogManager().v2SessionCatalog();
//        Identifier sourceIdent = toCatalogAndIdentifier(sourceTable, PARAMETERS[1].name(), v2SessionCatalog, sparkSession).identifier();
//        org.apache.spark.sql.catalyst.TableIdentifier sourceTableIdent = Spark3Util.toV1TableIdentifier(sourceIdent);
//        String db = sourceTableIdent.database().nonEmpty() ?
//                sourceTableIdent.database().get() :
//                sparkSession.sessionState().catalog().getCurrentDatabase();

//        System.out.println(list);
//        TableIdentifier sourceTableIdentWithDB = TableIdentifier.of(sourceTableIdent.table(), db);
//        PartitionSpec spec = SparkSchemaUtil.specForTable(sparkSession, sourceTableIdentWithDB.name());
//        Schema schema = SparkSchemaUtil.schemaForTable(sparkSession, sourceTable);
}