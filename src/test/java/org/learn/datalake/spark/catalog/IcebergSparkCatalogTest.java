package org.learn.datalake.spark.catalog;

import com.google.common.base.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

import java.util.List;

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
    protected static String hmsUri = "thrift://10.201.0.212:39083";
    protected static String table = "dlink_default.test_spark_tbl_1";
    private static String hiveHome = "/Users/deepexi/soft/apache-hive-3.1.2-bin";

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
    public void testAnotherSparkCatalog() {
        SparkConf sparkConf = new SparkConf()
                .set("spark.default.parallelism", "1")
//                MetastoreConf available in HMS 3.x
//                .set(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname(), defaultCatalogName)
                .set("metastore.catalog.default", defaultCatalogName)

                .set(METASTOREURIS.varname, hiveMetastoreURI)
//                spark.hadoop.+ hadoop configuration
//                .set("spark.hadoop." + METASTOREURIS.varname, hiveMetastoreURI)

                .set("spark.sql.warehouse.dir", "/tmp/")
//                HIVE_METASTORE_VERSION defined in HiveUtils.scala of Spark-hive jars
                .set("spark.sql.hive.metastore.version", "3.1.2")
                .set("spark.sql.hive.metastore.jars", String.format("%s/lib/*", hiveHome))

                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        sparkSession.sql("show databases").show();
        sparkSession.sql("use dlink_default").show();
    }

    @Test
    public void testIcebergSparkSessionCatalog() {
        SparkConf sparkConf = new SparkConf()
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog.spark_catalog.type", "hive")
                .set("spark.sql.catalog.spark_catalog.default-namespace", "default")
                .set("spark.sql.catalog.spark_catalog.uri", hiveMetastoreURI)
                .set("spark.sql.catalog.spark_catalog.warehouse", warehouse)
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.access.key", "admin1234")
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.secret.key", "admin1234")
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")
                .set("spark.sql.catalog.spark_catalog.hadoop.metastore.catalog.default", defaultCatalogName)
                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .set("spark.default.parallelism", "1")
                .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .set("spark.sql.hive.metastore.version", "3.1.2")
                .set("spark.sql.hive.metastore.jars",String.format("%s/lib/*",hiveHome))
                .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true") // support read data of timestamp-without-timezone
                .set("spark.sql.warehouse.dir", "warehouse")
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        sparkSession.sql("show databases").show();
//        sparkSession.sql("select * from spark_catalog.dlink_default.test_flink_hive_table_4").show();
        sparkSession.sql("drop table if exists dlink_default.test_spark_tbl_1");
        sparkSession.sql(String.format("create table if not exists dlink_default.test_spark_tbl_1(id int) USING iceberg TBLPROPERTIES ('iceberg.catalog'='spark_catalog')"));
        sparkSession.sql("insert into dlink_default.test_spark_tbl_1 values(1)");
        sparkSession.sql("select * from dlink_default.test_spark_tbl_1");
//        sparkSession.sql("drop table dlink_default.test_spark_tbl_1");
    }

    @Test
    public void testIcebergSparkCatalogNotWork() {
        SparkConf sparkConf = new SparkConf()
                .set("spark.sql.catalog." + anotherCatalogMappingName, "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".type", "hive")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".default-namespace", "default")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".uri", hiveMetastoreURI)
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".warehouse", warehouse)
                .set("spark.default.parallelism", "1")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.access.key", "admin1234")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.secret.key", "admin1234")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.metastore.catalog.default", defaultCatalogName)
                .set("metastore.catalog.default", defaultCatalogName)
                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .set("spark.sql.hive.metastore.version", "3.1.2")
                .set("spark.sql.hive.metastore.jars",String.format("%s/lib/*",hiveHome))
                .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true") // support read data of timestamp-without-timezone
                .set("spark.sql.warehouse.dir", "warehouse")
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        sparkSession.sql("set spark.sql.defaultCatalog=" + anotherCatalogMappingName);
        sparkSession.sql("show databases").show();
        sparkSession.sql("drop table if exists another_catalog.default.test_spark_tbl1");
        sparkSession.sql(String.format("create table if not exists another_catalog.default.test_spark_tbl1(id int) USING iceberg TBLPROPERTIES ('iceberg.catalog'='%s')", anotherCatalogMappingName));
        sparkSession.sql("insert into another_catalog.default.test_spark_tbl1 values(1)");
        sparkSession.sql("select * from another_catalog.default.test_spark_tbl1");
        sparkSession.sql("drop table another_catalog.default.test_spark_tbl1");
    }

    @Test
    public void testMultipleCatalogs() {
        String anotherCatalogMappingName = "another_catalog";
        String defaultHiveMetastoreURI = "thrift://10.201.1.2:9083";

        SparkConf sparkConf = new SparkConf()
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog.spark_catalog.type", "hive")
                .set("spark.sql.catalog.spark_catalog.default-namespace", "default")
                .set("spark.sql.catalog.spark_catalog.uri", hiveMetastoreURI)
                .set("spark.sql.catalog.spark_catalog.warehouse", warehouse)
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.access.key", "admin1234")
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.secret.key", "admin1234")
                .set("spark.sql.catalog.spark_catalog.hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")
                .set("spark.sql.catalog.spark_catalog.hadoop.metastore.catalog.default", defaultCatalogName)
                .set("spark.sql.hive.metastore.version", "3.1.2")
                .set("spark.sql.hive.metastore.jars", String.format("%s/lib/*", hiveHome))
                .set("spark.default.parallelism", "1")

                .set("spark.sql.catalog." + anotherCatalogMappingName, "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".type", "hive")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".default-namespace", "default")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".uri", defaultHiveMetastoreURI)
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".warehouse", warehouse)
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.access.key", "admin1234")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.secret.key", "admin1234")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")
                .set("spark.sql.catalog." + anotherCatalogMappingName + ".hadoop.metastore.catalog.default", defaultCatalogName)


                .set("spark.default.parallelism", "24")
                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true") // support read data of timestamp-without-timezone
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        String sourceTable = "test";
        sparkSession.sql("show databases;").show();
        sparkSession.sql("select * from spark_catalog.dlink_default.test_flink_hive_table_4 limit 10").show();
        sparkSession.sql("set spark.sql.defaultCatalog=" + anotherCatalogMappingName);
        sparkSession.sql("show databases;").show();
        sparkSession.sql("show tables;").show();

        SessionCatalog sessionCatalog = sparkSession.sessionState().catalog();

        CatalogPlugin v2SessionCatalog = sparkSession.sessionState().catalogManager().v2SessionCatalog();
        Identifier sourceIdent = toCatalogAndIdentifier(sourceTable, PARAMETERS[1].name(), v2SessionCatalog, sparkSession).identifier();
        org.apache.spark.sql.catalyst.TableIdentifier sourceTableIdent = Spark3Util.toV1TableIdentifier(sourceIdent);
        String db = sourceTableIdent.database().nonEmpty() ?
                sourceTableIdent.database().get() :
                sparkSession.sessionState().catalog().getCurrentDatabase();

        List<Row> list = sparkSession.sql("select * from spark_catalog.test_federal_db.test_hive_table,another_catalog.default.wns_ctl_tt_5 limit 10").collectAsList();
        System.out.println(list);
//        TableIdentifier sourceTableIdentWithDB = TableIdentifier.of(sourceTableIdent.table(), db);
//        PartitionSpec spec = SparkSchemaUtil.specForTable(sparkSession, sourceTableIdentWithDB.name());
//        Schema schema = SparkSchemaUtil.schemaForTable(sparkSession, sourceTable);


//        Map<String, String> dstProperties = new HashMap<>();
//        dstProperties.put("uri", targetHiveMetastoreURI);
//        Configuration dstConfig = new Configuration();
//        HiveCatalog catalog = new HiveCatalog();
//        dstConfig.set("metastore.catalog.default", targetCatalogMappingName);
//        catalog.setConf(dstConfig);
//        catalog.initialize(targetCatalogMappingName, dstProperties);
//
//
//        System.out.println("==========> list iceberg catalog databases");
//        catalog.listNamespaces().forEach(System.out::println);
//
//        System.out.println("==========> create iceberg table");
//        String[] tmp = targetIcebergTable.split("\\.");
//        System.out.println("table:     " + tmp[0] + "." + tmp[1]);
//        TableIdentifier tableIdent = TableIdentifier.of(tmp[0], tmp[1]);
//        Table targetTable;


//        if (!false) {
//            // 不需要检查表是否存在，则直接创建
//            targetTable = catalog.createTable(tableIdent, schema, spec);
//        } else {
//            if (catalog.tableExists(tableIdent)) {
//                targetTable = catalog.loadTable(tableIdent);
//                System.out.println("==========> load iceberg table");
//            } else {
//                targetTable = catalog.createTable(tableIdent, schema, spec);
//            }
//        }
//        String location = targetTable.location();
//        System.out.println("location:  " + location);

        System.out.println("==========> start to generate metadata");
    }

    private String getMetadataLocation(Table table) {
        String defaultValue = table.location() + "/metadata";
        return table.properties().getOrDefault(TableProperties.WRITE_METADATA_LOCATION, defaultValue);
    }

//    protected static void sql(String query, Object... args) {
//        spark.sql(String.format(query, args)).show();
//    }
}
