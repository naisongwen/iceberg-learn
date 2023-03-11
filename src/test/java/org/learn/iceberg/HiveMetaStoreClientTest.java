package org.learn.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.required;

public class HiveMetaStoreClientTest extends BaseTest {

    @Test
    public void testHive() throws TException {
        String dbName = "test_s3_db_4";
        Database database = new Database(dbName, "", "s3a://dlink-public/" + dbName, null);
        //location determined by hive.metastore.warehouse.dir
//        hiveMetaStoreClient.createDatabase(database);
        Table table = new Table();
        table.setSd(new StorageDescriptor());
        table.setTableName("test_s3_table_1");
        table.setDbName(dbName);
        table.setTableType(TableType.EXTERNAL_TABLE.name());
//        hiveMetaStoreClient.createTable(table);

        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("property-version", "1");
        properties.put("catalog-type", "hive");
        properties.put("uri", hmsUri);
        CatalogLoader catalogLoader = CatalogLoader.hive("HIVE_CATALOG", hiveConf, properties);
        Catalog catalog = catalogLoader.loadCatalog();
//        properties.put("warehouse",warehouse);
        final Schema SCHEMA = new Schema(
                required(3, "id", Types.IntegerType.get(), "unique ID"),
                required(4, "data", Types.StringType.get())
        );
        TableIdentifier tbl = TableIdentifier.of(dbName, "tbl_4");

        catalog.createTable(tbl,SCHEMA,null,"s3a://dlink-public/test_s3_db_4/tbl_4/",properties);
        org.apache.iceberg.Table loadTable=catalog.loadTable(tbl);

        hiveMetaStoreClient.getAllDatabases().forEach(System.out::println);

//        org.apache.hadoop.hive.metastore.api.Table table = hiveMetaStoreClient.getTable("hive", "hexf07", "hive_test05");
//        System.out.println(table);
    }

    @Test
    public void testMinIO() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.access.key", "admin1234");
        conf.set("fs.s3a.secret.key", "admin1234");
        conf.set("fs.s3a.endpoint", "http://10.201.0.212:32000");
        conf.set("fs.s3a.path.style.access", "true");
        String warehouse = "s3a://faas-ethan/iceberg/warehouse";

        S3AFileSystem s3AFileSystem = (S3AFileSystem) S3AFileSystem.get(new URI(warehouse), conf);
        s3AFileSystem.delete(new Path(warehouse), true);
    }

    @Test
    public void testS3() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.access.key", "AKIA4ZNT6QH3HFN6OYNO");
        conf.set("fs.s3a.secret.key", "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");
        conf.set("fs.s3a.endpoint", "http://s3.cn-northwest-1.amazonaws.com.cn");
        conf.set("fs.s3a.endpoint.region", "cn-northwest-1");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        String warehouse = "s3a://dlink-public/";

        S3AFileSystem s3AFileSystem = (S3AFileSystem) S3AFileSystem.newInstance(new URI(warehouse), conf);
        String testPath = warehouse + "test";
        s3AFileSystem.create(new Path(testPath));
        RemoteIterator<LocatedFileStatus> remoteIterator = s3AFileSystem.listFiles(new Path(warehouse), true);
        while (remoteIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = remoteIterator.next();
            System.out.println(locatedFileStatus.getPath());
        }
    }
}
