package org.learn.datalake.spark.catalog;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
//import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;


public class IcebergSparkCatalogTest {
    protected static final DataType STRING_MAP = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);
    private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
            ProcedureParameter.required("table", DataTypes.StringType),
            ProcedureParameter.required("source_table", DataTypes.StringType),
            ProcedureParameter.optional("partition_filter", STRING_MAP)
    };
    private static String hiveMetastoreURI = "thrift://10.201.0.212:39083";
    private static String targetCatalogMappingName = "spark_catalog_2";
    private static String targetHiveMetastoreURI = "thrift://localhost:10000";
    private static String warehouse = "s3a://faas-ethan/warehouse";

    private static String defaultCatalogName="hzj_hdfs_dev_8_1974";
    private static String hiveHome="/Users/deepexi/soft/apache-hive-3.1.2-bin";

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
//                .set(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname(), defaultCatalogName)
                .set("metastore.catalog.default", defaultCatalogName)

                .set(METASTOREURIS.varname, hiveMetastoreURI)
//                spark.hadoop.+ hadoop configuration
//                .set("spark.hadoop." + METASTOREURIS.varname, hiveMetastoreURI)

                .set("spark.sql.warehouse.dir", "warehouse")
//                HIVE_METASTORE_VERSION defined in HiveUtils.scala of Spark-hive jars
                .set("spark.sql.hive.metastore.version", "3.1.2")
                .set("spark.sql.hive.metastore.jars",String.format("%s/lib/*",hiveHome))

                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        sparkSession.sql("show databases").show();
    }

    @Test
    public void testIcebergHiveCatalog(){
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
        String catalogMappingName = "spark_catalog";
        SparkConf sparkConf = new SparkConf()
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog." + catalogMappingName, "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog." + catalogMappingName + ".type", "hive")
                .set("spark.sql.catalog." + catalogMappingName + ".default-namespace", "default")
                .set("spark.sql.catalog." + catalogMappingName + ".uri", hiveMetastoreURI)
                .set("spark.sql.catalog." + catalogMappingName + ".warehouse", warehouse)
                .set("spark.default.parallelism", "1")
                .set("spark.hadoop." + METASTOREURIS.varname, hiveMetastoreURI)
                .set("spark.hadoop.fs.s3a.endpoint", hiveMetastoreURI)
                .set("spark.hadoop.fs.s3a.access.key", "admin1234")
                .set("spark.hadoop.fs.s3a.secret.key", "admin1234")
                .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .set("spark.hadoop.fs.s3a.endpoint", "http://10.201.0.212:32000")
                .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.sql.warehouse.dir", "warehouse")
                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
//                .set("metastore.catalog.default", defaultCatalogName)
//                .set("spark.sql.hive.metastore.version", "3.1.2")
//                .set("spark.sql.hive.metastore.jars",String.format("%s/lib/*",hiveHome))
                .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true") // support read data of timestamp-without-timezone
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        sparkSession.sql("show databases").show();
        sparkSession.sql("create table if not exists test_spark_tbl(id int) USING iceberg");
        sparkSession.sql("select * from test_spark_tbl");
        sparkSession.sql("drop table if exists test_spark_tbl");
    }

    public void main(String[] args) {
        String catalogMappingName = "spark_catalog";
        SparkConf sparkConf = new SparkConf()
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog." + catalogMappingName, "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog." + catalogMappingName + ".type", "hive")
                .set("spark.sql.catalog." + catalogMappingName + ".default-namespace", "default")
                .set("spark.sql.catalog." + catalogMappingName + ".uri", hiveMetastoreURI)
                .set("spark.sql.catalog." + catalogMappingName + ".warehouse", warehouse)

                .set("spark.sql.catalog." + targetCatalogMappingName, "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog." + targetCatalogMappingName + ".type", "hive")
                .set("spark.sql.catalog." + targetCatalogMappingName + ".default-namespace", "iceberg_db")
                .set("spark.sql.catalog." + targetCatalogMappingName + ".uri", targetHiveMetastoreURI)
                .set("spark.sql.catalog." + catalogMappingName + ".warehouse", warehouse)

                .set("spark.default.parallelism", "24")
                .set("metastore.catalog.default", catalogMappingName)
                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true") // support read data of timestamp-without-timezone
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(this.getClass().getSimpleName());

        String sourceTable = "test";
        String targetIcebergTable = "default.test";
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        sparkSession.sql("create table hive_test_table_1 (name string)");
        SessionCatalog sessionCatalog = sparkSession.sessionState().catalog();

        CatalogPlugin v2SessionCatalog = sparkSession.sessionState().catalogManager().v2SessionCatalog();
        Identifier sourceIdent = toCatalogAndIdentifier(sourceTable, PARAMETERS[1].name(), v2SessionCatalog, sparkSession).identifier();
        org.apache.spark.sql.catalyst.TableIdentifier sourceTableIdent = Spark3Util.toV1TableIdentifier(sourceIdent);
        String db = sourceTableIdent.database().nonEmpty() ?
                sourceTableIdent.database().get() :
                sparkSession.sessionState().catalog().getCurrentDatabase();

        List<Row> list = sparkSession.sql("select * from spark_catalog.default.test1t,spark_catalog_2.iceberg_db.cx_source limit 10").collectAsList();
        System.out.println(list);
//        TableIdentifier sourceTableIdentWithDB = TableIdentifier.of(sourceTableIdent.table(), db);
//        PartitionSpec spec = SparkSchemaUtil.specForTable(sparkSession, sourceTableIdentWithDB.name());
//        Schema schema = SparkSchemaUtil.schemaForTable(sparkSession, sourceTable);


        Map<String, String> dstProperties = new HashMap<>();
        dstProperties.put("uri", targetHiveMetastoreURI);
        Configuration dstConfig = new Configuration();
        HiveCatalog catalog = new HiveCatalog();
        dstConfig.set("metastore.catalog.default", targetCatalogMappingName);
        catalog.setConf(dstConfig);
        catalog.initialize(targetCatalogMappingName, dstProperties);


        System.out.println("==========> list iceberg catalog databases");
        catalog.listNamespaces().forEach(System.out::println);

        System.out.println("==========> create iceberg table");
        String[] tmp = targetIcebergTable.split("\\.");
        System.out.println("table:     " + tmp[0] + "." + tmp[1]);
        TableIdentifier tableIdent = TableIdentifier.of(tmp[0], tmp[1]);
        Table targetTable;
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

    protected static Spark3Util.CatalogAndIdentifier toCatalogAndIdentifier(String identifierAsString, String argName,
                                                                            CatalogPlugin catalog, SparkSession spk) {
        Preconditions.checkArgument(identifierAsString != null && !identifierAsString.isEmpty(),
                "Cannot handle an empty identifier for argument %s", argName);

        return Spark3Util.catalogAndIdentifier("identifier for arg " + argName, spk, identifierAsString, catalog);
    }

    private String getMetadataLocation(Table table) {
        String defaultValue = table.location() + "/metadata";
        return table.properties().getOrDefault(TableProperties.WRITE_METADATA_LOCATION, defaultValue);
    }

//    protected static void sql(String query, Object... args) {
//        spark.sql(String.format(query, args)).show();
//    }

}
