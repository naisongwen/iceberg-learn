import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.learn.datalake.common.SimpleDataUtil;

import java.util.Locale;
import java.util.Map;

public class TestHiveCatalog {
    private static final String primaryHost = "10.201.0.121";
    private static final String warehouse = String.format("hdfs://%s:9003/user/hive/warehouse/iceberg/", primaryHost);
    private static final String defaultFS = String.format("hdfs://%s:9003", primaryHost);
    private static final String uri = String.format("thrift://%s:9083", primaryHost);

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
