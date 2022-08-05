package org.learn.datalake.spark.catalog;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iceberg.types.Types.NestedField.required;

//Reference TestHadoopCatalog
public class IcebergCatalogTest {
    final Schema schema = new Schema(
            required(1, "id", Types.IntegerType.get(), "unique ID"),
            required(2, "data", Types.StringType.get())
    );

    @Test
    public void testIcebergHiveCatalog() {
        Configuration conf = new Configuration();
        String catalogName = "default";
        String thriftUri = "thrift://localhost:9083";
//        File warehouse = new File("warehouse");
//        try {
//            if (warehouse.exists())
//                FileUtils.deleteDirectory(warehouse);
//        } catch (Exception e) {
//        }
//        warehouse.mkdirs();
//        String warehouseLocation = warehouse.getAbsolutePath();

        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(conf);
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", thriftUri);
//        properties.put("warehouse", warehouseLocation);
        hiveCatalog.initialize(catalogName, properties);
        String db="test_db_2";
        String tbl="test_table";
        TableIdentifier tableIdentifier=TableIdentifier.of(Namespace.of(db),tbl);
        hiveCatalog.createNamespace(Namespace.of(db));
        hiveCatalog.createTable(tableIdentifier,schema);
        Table table=hiveCatalog.loadTable(tableIdentifier);
        CloseableIterable<Record> iterable = IcebergGenerics.read(table).build();

        for (Record record : iterable) {
            System.out.println(record);
        }
    }

    @Test
    public void testHadoopCatalog() {
        File warehouse = new File("warehouse");
        try {
            if (warehouse.exists())
                FileUtils.deleteDirectory(warehouse);
        } catch (Exception e) {
        }
        warehouse.mkdirs();
        String warehouseLocation = warehouse.getAbsolutePath();
        Configuration conf = new Configuration();
        HadoopCatalog hadoopCatalog = new HadoopCatalog(conf, warehouseLocation);

        TableIdentifier tbl1 = TableIdentifier.of("db", "tbl1");
        TableIdentifier tbl2 = TableIdentifier.of("db", "tbl2");
        TableIdentifier tbl3 = TableIdentifier.of("db", "ns1", "tbl3");
        TableIdentifier tbl4 = TableIdentifier.of("db", "metadata", "metadata");

//        hadoopCatalog.listTables()
        Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
                hadoopCatalog.buildTable(t, schema)
                        .withPartitionSpec(PartitionSpec.unpartitioned())
                        .create());

        Table table = hadoopCatalog.loadTable(tbl1);
        Schema  schema=table.schema();
        List<TableIdentifier> tbls1 = hadoopCatalog.listTables(Namespace.of("db"));
        Set<String> tblSet = Sets.newHashSet(tbls1.stream().map(t -> t.name()).iterator());
        Assert.assertEquals(2, tblSet.size());
        Assert.assertTrue(tblSet.contains("tbl1"));
        Assert.assertTrue(tblSet.contains("tbl2"));

        List<TableIdentifier> tbls2 = hadoopCatalog.listTables(Namespace.of("db", "ns1"));
        Assert.assertEquals("table identifiers", 1, tbls2.size());
        Assert.assertEquals("table name", "tbl3", tbls2.get(0).name());

        List<Namespace> nsp1 = hadoopCatalog.listNamespaces();
        Set<String> tblSet3 = Sets.newHashSet(nsp1.stream().map(t -> t.toString()).iterator());
        Assert.assertEquals(1, tblSet3.size());
        Assert.assertTrue(tblSet3.contains("db"));

        List<Namespace> nsp2 = hadoopCatalog.listNamespaces(Namespace.of("db"));
        Set<String> tblSet4 = Sets.newHashSet(nsp2.stream().map(t -> t.toString()).iterator());
        Assert.assertEquals(2, tblSet4.size());
        Assert.assertTrue(tblSet4.contains("db.ns1"));
        Assert.assertTrue(tblSet4.contains("db.metadata"));
    }
}
