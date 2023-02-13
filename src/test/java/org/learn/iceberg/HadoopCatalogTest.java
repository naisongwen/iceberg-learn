package org.learn.iceberg;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;

import java.io.File;
import java.util.List;
import java.util.Set;

import static org.apache.iceberg.types.Types.NestedField.required;

//Reference TestHadoopCatalog
public class HadoopCatalogTest {
    static final Schema SCHEMA = new Schema(
            required(1, "id", Types.IntegerType.get(), "unique ID"),
            required(2, "data", Types.StringType.get())
    );

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        File warehouse = new File("warehouse");
        try {
            if (warehouse.exists())
                FileUtils.deleteDirectory(warehouse);
        } catch (Exception e) {
        }
        warehouse.mkdirs();
        String warehouseLocation = warehouse.getAbsolutePath();
        warehouseLocation = "s3a://faas-ethan/warehouse/";
        HadoopCatalog hadoopCatalog = new HadoopCatalog(conf, warehouseLocation);

        TableIdentifier tableIdentifier = TableIdentifier.of("default", "sample_hadoop_table_2");

        hadoopCatalog.buildTable(tableIdentifier, SCHEMA)
                .withPartitionSpec(PartitionSpec.unpartitioned())
                .create();

        Table table = hadoopCatalog.loadTable(tableIdentifier);
        Schema schema = table.schema();
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
