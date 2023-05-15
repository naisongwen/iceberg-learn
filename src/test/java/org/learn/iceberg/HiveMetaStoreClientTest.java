package org.learn.iceberg;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.required;

public class HiveMetaStoreClientTest extends BaseTest {

    @Test
    public void testCreateCatalog() throws TException {
        org.apache.hadoop.hive.metastore.api.Catalog catalog = new org.apache.hadoop.hive.metastore.api.Catalog("c1", "s3a://bucket1/dlink-090f753804aa4ea299d0e45852ad5709-adf04df0/sddwwe_714");
        hiveMetaStoreClient.createCatalog(catalog);
    }

    @Test
    public void testCreateTable() throws TException {
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

        catalog.createTable(tbl, SCHEMA, null, "s3a://dlink-public/test_s3_db_4/tbl_4/", properties);
        org.apache.iceberg.Table loadTable = catalog.loadTable(tbl);

        hiveMetaStoreClient.getAllDatabases().forEach(System.out::println);

//        org.apache.hadoop.hive.metastore.api.Table table = hiveMetaStoreClient.getTable("hive", "hexf07", "hive_test05");
//        System.out.println(table);
    }

    private Table newTable(String dbName, String tableName) {
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(Arrays.asList(
                new FieldSchema("column1", "int", ""),
                new FieldSchema("column2", "string", "")
        ));
//        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
//        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
//        sd.setSerdeInfo(new SerDeInfo(null, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", new HashMap<>()));

        Table tbl = new Table();
        tbl.setDbName(dbName);
        tbl.setTableName(tableName);
        tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
        tbl.setSd(sd);
        tbl.setPartitionKeys(new ArrayList<>());
        tbl.setCreateTime((int) (System.currentTimeMillis() / 1000L));
        tbl.setLastAccessTime(0);
//        tbl.setOwner(USER_NAME);
        tbl.setRetention(0);
        tbl.setViewOriginalText(null);
        tbl.setViewExpandedText(null);
        return tbl;
    }

    @Test
    public void testCreateTableForPerformance() throws TException {
        String dbName = "test_database_performance_5";
        Database database = new Database(dbName, "", "s3a://test/" + dbName, null);
        //location determined by hive.metastore.warehouse.dir
        hiveMetaStoreClient.createDatabase(database);
        long start=System.currentTimeMillis();
        for (int i = 1; i < 100; i++) {
            String tblName = "test_iceberg_table_" + i;
            Table table=newTable(dbName,tblName);
            hiveMetaStoreClient.createTable(table);
            System.out.println("create table "+tblName);
        }
        long end=System.currentTimeMillis();
        System.out.println("100 created tables took "+(end-start)/1000);
        //100 created tables took 9
        hiveMetaStoreClient.close();
    }


    @Test
    public void testGetTable() throws TException {
        String dbName = "test_database_performance_4";
        String tblName = "test_iceberg_table_1";
        Table tbl=hiveMetaStoreClient.getTable(dbName,tblName);
        assert tbl!=null;
    }
}
