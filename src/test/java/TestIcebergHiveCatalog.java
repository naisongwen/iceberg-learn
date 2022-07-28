import com.google.common.collect.Maps;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.learn.datalake.common.SimpleDataUtil;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class TestIcebergHiveCatalog {
    private static final String primaryHost = "10.201.0.121";
    private static final String warehouse = String.format("hdfs://%s:9003/user/hive/warehouse/iceberg/", primaryHost);
    private static final String defaultFS = String.format("hdfs://%s:9003", primaryHost);
    private static final String uri = String.format("thrift://%s:9083", primaryHost);

    @Test
    public void testCreateHiveMappingTable() throws DatabaseAlreadyExistException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, environmentSettings);
        HiveConf hiveConf=new HiveConf();
        String hmsUri="thrift://localhost:9083";
        hiveConf.set("hive.metastore.uris", hmsUri);
        hiveConf.set("metastore.catalog.default", "hive");
        hiveConf.set("hive.metastore.client.capability.check", "false");
        hiveConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hiveConf.set("fs.s3a.access.key", "admin1234");
        hiveConf.set("fs.s3a.connection.ssl.enabled", "false");
        hiveConf.set("fs.s3a.secret.key", "admin1234");
        hiveConf.set("fs.s3a.endpoint", "http://10.201.0.212:32000");
        HiveCatalog hiveCatalog=new HiveCatalog("test_catalog_name","test_db",hiveConf, HiveShimLoader.getHiveVersion());
        hiveCatalog.open();
        tableEnvironment.registerCatalog(hiveCatalog.getName(),hiveCatalog);
        Map<String, String> map = new HashMap<>();
        map.put("test", "test");
        String tblName="test_iceberg_table_2";
//        CatalogDatabaseImpl catalogDatabase = new CatalogDatabaseImpl(map, "test");
//        hiveCatalog.createDatabase( "test_database", catalogDatabase,true);
//        tableEnvironment.useCatalog(hiveCatalog.getName());
//        tableEnvironment.useDatabase("test_database");
        tableEnvironment.executeSql(String.format("drop table if exists %s",tblName));
        String sql= String.format("CREATE TABLE %s(" +
                "     id INT,\n" +
                "     data string,\n" +
                "      primary key(id) not enforced\n" +
                "     ) WITH (" +
                "               'connector' = 'iceberg',\n" +
                "                'format-version' = '2',\n" +
                "                'engine.hive.enabled' = 'true',\n" +
                "                'write.upsert.enabled' = 'true',\n" +
                "                'uri' = '%s',\n" +
                "                'catalog-name'='%s',\n" +
                "                'catalog-type'='hive',\n" +
                "                'is_generic' = 'false',\n" +
                "                'catalog-database' = '%s',\n" +
                "                'catalog-table' = '%s',\n" +
                "                'warehouse'='%s'\n" +
                              ")",tblName,hmsUri,hiveCatalog.getName(),"default",tblName,"s3a://faas-ethan/hive_db/");
        tableEnvironment.executeSql(sql);
        tableEnvironment.executeSql(String.format("insert into %s values(1,'a')",tblName));
        tableEnvironment.executeSql(String.format("select * from %s",tblName)).print();
        hiveCatalog.close();
    }

    @Test
    public void testCreateHiveIcebergTable() throws DatabaseAlreadyExistException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, environmentSettings);
        Configuration hiveConf=new Configuration();
        String hmsUri="thrift://localhost:9083";
        hiveConf.set("hive.metastore.uris", hmsUri);
        hiveConf.set("hive.metastore.warehouse.dir", "s3a://faas-ethan/");
        hiveConf.set("metastore.catalog.default", "hive");
        hiveConf.set("hive.metastore.client.capability.check", "false");
        hiveConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hiveConf.set("fs.s3a.access.key", "admin1234");
        hiveConf.set("fs.s3a.connection.ssl.enabled", "false");
        hiveConf.set("fs.s3a.secret.key", "admin1234");
        hiveConf.set("fs.s3a.endpoint", "http://10.201.0.212:32000");

        Map<String, String> properties = new HashMap<>();
        properties.put("test", "test");
        CatalogLoader catalogLoader=CatalogLoader.hive("hive", hiveConf, properties);
        FlinkCatalog flinkCatalog=new FlinkCatalog("test_catalog_name","test_db", Namespace.empty(),catalogLoader,false);
        flinkCatalog.open();
        tableEnvironment.registerCatalog(flinkCatalog.getName(),flinkCatalog);
        tableEnvironment.useCatalog(flinkCatalog.getName());
        String tblName="test_iceberg_table_15";
        CatalogDatabaseImpl catalogDatabase = new CatalogDatabaseImpl(Maps.newHashMap(), "test_database");
        flinkCatalog.createDatabase( "test_database", catalogDatabase,true);
        tableEnvironment.useDatabase("test_database");
        tableEnvironment.executeSql(String.format("drop table if exists %s",tblName));
        String sql= String.format("CREATE TABLE %s(" +
                "     id BIGINT COMMENT 'unique id'," +
                "     data STRING" +
                "     ) WITH (" +
                ")",tblName);
        tableEnvironment.executeSql(sql);
        tableEnvironment.executeSql(String.format("insert into %s values(1,'a')",tblName));
        tableEnvironment.executeSql(String.format("select * from %s",tblName)).print();
        flinkCatalog.close();
    }

    @Test
    public void testS3Table() {
        CatalogLoader catalogLoader;
        FileFormat format = FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name())
                .of("type", "iceberg")
                .of(TableProperties.FORMAT_VERSION, "1")
                .of("catalog-type", "hive")
                .of("warehouse", "s3a://wwwx")
                .of("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                .of("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .of("lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager")
                .of("lock.table", "myGlueLockTable")
                .of("uri", uri);

        Configuration cfg = new Configuration();
        String defaultFS = "hdfs://10.201.0.121:9003";
        cfg.set("fs.defaultFS", defaultFS);

        catalogLoader = CatalogLoader.hive("iceberg_default", cfg, properties);
        TableIdentifier dataIdentifier = TableIdentifier.of("default", "iceberg_test_s3_table2");
        catalogLoader.loadCatalog().dropTable(dataIdentifier);
        catalogLoader.loadCatalog().createTable(dataIdentifier, SimpleDataUtil.SCHEMA);
    }

    @Test
    public void testDropTable() {
        CatalogLoader catalogLoader;
        FileFormat format = FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name())
                .of("type", "iceberg")
                .of(TableProperties.FORMAT_VERSION, "1")
                .of("catalog-type", "hive")
                .of("warehouse", warehouse)
                .of("hive-conf-dir", "/data5/flink/hive/conf/")
                .of("uri", uri);

        Configuration cfg = new Configuration();
        catalogLoader = CatalogLoader.hive("iceberg_default", cfg, properties);
        TableIdentifier dataIdentifier = TableIdentifier.of("default", "iceberg_test_table2");
        catalogLoader.loadCatalog().dropTable(dataIdentifier);
        catalogLoader.loadCatalog().createTable(dataIdentifier, SimpleDataUtil.SCHEMA);
    }
}
