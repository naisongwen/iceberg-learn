package org.learn.datalake.spark.catalog;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CatalogFederal {
    private static String warehousePath = "hdfs://10.201.1.128:9000/user/hive/warehouse";
    private static String hiveMetastoreURI = "thrift://10.201.1.128:9083";
    private static String targetWarehousePath = "hdfs://10.201.0.82:9000/cjtest/catalogmanager";
    private static String targetHiveMetastoreURI = "thrift://10.201.0.212:49153";

    private static String catalogMappingName="spark_catalog";
    private static String targetCatalogMappingName="spark_catalog_2";

    protected static final DataType STRING_MAP = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog." + catalogMappingName, "org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog." + catalogMappingName + ".type", "hive")
                .set("spark.sql.catalog." + catalogMappingName + ".default-namespace", "default")
                .set("spark.sql.catalog." + catalogMappingName + ".uri",targetHiveMetastoreURI)

                .set("spark.sql.catalog." + targetCatalogMappingName, "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog." + targetCatalogMappingName + ".type", "hive")
                .set("spark.sql.catalog." + targetCatalogMappingName + ".default-namespace", "iceberg_db")
                .set("spark.sql.catalog." + targetCatalogMappingName + ".uri",targetHiveMetastoreURI)
                .set("spark.default.parallelism", "24")
                .set("hive.metastore.uris",hiveMetastoreURI)
                .set("spark.sql.warehouse.dir",warehousePath)
//                .set("metastore.catalog.default",catalogMappingName)
                .set("parquet.metadata.read.parallelism", "1")// set parallelism to read metadata
                .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true") // support read data of timestamp-without-timezone
                .setMaster("local[*]") // spark-submit should remove this
                .setAppName(CatalogFederal.class.getSimpleName());

        String sourceTable="test";
        String targetIcebergTable="default.test";
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        SessionCatalog sessionCatalog=sparkSession.sessionState().catalog();

        CatalogPlugin v2SessionCatalog = sparkSession.sessionState().catalogManager().v2SessionCatalog();
        Identifier sourceIdent= toCatalogAndIdentifier(sourceTable, PARAMETERS[1].name(), v2SessionCatalog, sparkSession).identifier();
        org.apache.spark.sql.catalyst.TableIdentifier sourceTableIdent = Spark3Util.toV1TableIdentifier(sourceIdent);
        String db = sourceTableIdent.database().nonEmpty() ?
                sourceTableIdent.database().get() :
                sparkSession.sessionState().catalog().getCurrentDatabase();

        List<Row> list=sparkSession.sql("select * from spark_catalog.default.test1t,spark_catalog_2.iceberg_db.cx_source limit 10").collectAsList();
        System.out.println(list);
//        TableIdentifier sourceTableIdentWithDB = TableIdentifier.of(sourceTableIdent.table(), db);
//        System.out.println("==========> get hive table partition spec");
//        PartitionSpec spec = SparkSchemaUtil.specForTable(sparkSession, sourceTableIdentWithDB.name());
//        System.out.println("==========> get hive table schema");
//        Schema schema = SparkSchemaUtil.schemaForTable(sparkSession, sourceTable);

        System.out.println("==========> start to create iceberg catalog");
        System.out.println("catalog:    " + targetCatalogMappingName);
        System.out.println("uri:        " + targetHiveMetastoreURI);
        System.out.println("warehouse:  " + targetWarehousePath);

        Map<String, String> dstProperties = new HashMap<>();
        dstProperties.put("warehouse",  targetWarehousePath);
        dstProperties.put("uri",  targetHiveMetastoreURI);
        Configuration dstConfig = new Configuration();
        HiveCatalog catalog = new HiveCatalog();
        dstConfig.set("metastore.catalog.default", targetCatalogMappingName);
        catalog.setConf(dstConfig);
        catalog.initialize(targetCatalogMappingName, dstProperties);


        System.out.println("==========> list iceberg catalog databases");
        catalog.listNamespaces().forEach(System.out::println);

        System.out.println("==========> create iceberg table");
        String []tmp = targetIcebergTable.split("\\.");
        System.out.println("table:     " + tmp[0] + "." + tmp[1]);
        TableIdentifier tableIdent =TableIdentifier.of(tmp[0], tmp[1]);
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

    private String getMetadataLocation(Table table) {
        String defaultValue = table.location() + "/metadata";
        return table.properties().getOrDefault(TableProperties.WRITE_METADATA_LOCATION, defaultValue);
    }

    private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
            ProcedureParameter.required("table", DataTypes.StringType),
            ProcedureParameter.required("source_table", DataTypes.StringType),
            ProcedureParameter.optional("partition_filter", STRING_MAP)
    };

    protected static Spark3Util.CatalogAndIdentifier toCatalogAndIdentifier(String identifierAsString, String argName,
                                                                            CatalogPlugin catalog, SparkSession spk) {
        Preconditions.checkArgument(identifierAsString != null && !identifierAsString.isEmpty(),
                "Cannot handle an empty identifier for argument %s", argName);

        return Spark3Util.catalogAndIdentifier("identifier for arg " + argName, spk, identifierAsString, catalog);
    }

//    protected static void sql(String query, Object... args) {
//        spark.sql(String.format(query, args)).show();
//    }

}
