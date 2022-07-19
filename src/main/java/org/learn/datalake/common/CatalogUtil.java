package org.learn.datalake.common;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;

/**
 * @author wennaisong
 */
public class CatalogUtil {

    public static Table getOrCreateIcebergTable(CatalogLoader catalogLoader, TableIdentifier identifier, Schema schema, boolean cleanWarehouse) throws IOException, URISyntaxException {
        Table table = null;
        Catalog catalog = catalogLoader.loadCatalog();
        try {
            //NOTE:make sure the database already exists
            table = catalog.loadTable(identifier);
            if (!cleanWarehouse) {
                return table;
            }
            //NOTE:make sure table dropped before deleting data
            catalog.dropTable(identifier);
        } catch (Exception e) {
            catalog.dropTable(identifier, false);
//            if (table != null) {
//                String location = table.location();
//                FileSystem fs = FileSystem.get(new URI(defaultFS), getHiveConf());
//                fs.delete(new Path(location), true);
//            }
        }
        table = catalog.createTable(identifier, schema);//, null, warehouse, null);
        return table;
    }

    static Configuration getHiveConf(String catalogName, String defaultFS) {
        Configuration cfg = new Configuration();
        cfg.set("metastore.catalog.default", catalogName);
        cfg.set("fs.defaultFS", defaultFS);
//        cfg.set("hive.metastore.warehouse.dir", "/user/hive/warehouse");
//        cfg.set("fs.s3a.access.key", "minio");
//        cfg.set("fs.s3a.secret.key", "minio123");
//        cfg.set("fs.s3a.endpoint", "http://10.201.0.212:34345");
        return cfg;
    }

    public static CatalogLoader getHiveCatalogLoader(String catalogName, String warehouse, String thriftUri) {
        CatalogLoader catalogLoader;
        FileFormat format = FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name())
                .of("type", "iceberg")
                .of(TableProperties.FORMAT_VERSION, "1")
                .of("catalog-type", "hive")
                .of("warehouse", warehouse)
                .of("uri", thriftUri);

        catalogLoader = CatalogLoader.hive(catalogName, getHiveConf(catalogName, warehouse), properties);
        return catalogLoader;
    }
}
