package org.learn.datalake.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.iceberg.types.Types.NestedField.required;

public class HadoopCatalogExample {
    static final Schema SCHEMA = new Schema(
            required(3, "id", Types.IntegerType.get(), "unique ID"),
            required(4, "data", Types.StringType.get())
    );

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        TemporaryFolder temp = new TemporaryFolder();
        temp.create();
        String warehousePath = temp.newFolder().getAbsolutePath();
        System.out.println("warehousePath:" + warehousePath);
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

        TableIdentifier tbl1 = TableIdentifier.of("db", "tbl1");
        TableIdentifier tbl2 = TableIdentifier.of("db", "tbl2");
        TableIdentifier tbl3 = TableIdentifier.of("db", "ns1", "tbl3");
        TableIdentifier tbl4 = TableIdentifier.of("db", "metadata", "metadata");

        Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
                catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
        );

        List<TableIdentifier> tbls1 = catalog.listTables(Namespace.of("db"));
        Set<String> tblSet = Sets.newHashSet(tbls1.stream().map(t -> t.name()).iterator());
        Assert.assertEquals(2, tblSet.size());
        Assert.assertTrue(tblSet.contains("tbl1"));
        Assert.assertTrue(tblSet.contains("tbl2"));

        List<TableIdentifier> tbls2 = catalog.listTables(Namespace.of("db", "ns1"));
        Assert.assertEquals("table identifiers", 1, tbls2.size());
        Assert.assertEquals("table name", "tbl3", tbls2.get(0).name());

        List<Namespace> nsp1 = catalog.listNamespaces();
        Set<String> tblSet3 = Sets.newHashSet(nsp1.stream().map(t -> t.toString()).iterator());
        Assert.assertEquals(1, tblSet3.size());
        Assert.assertTrue(tblSet3.contains("db"));

        List<Namespace> nsp2 = catalog.listNamespaces(Namespace.of("db"));
        Set<String> tblSet4 = Sets.newHashSet(nsp2.stream().map(t -> t.toString()).iterator());
        Assert.assertEquals(2, tblSet4.size());
        Assert.assertTrue(tblSet4.contains("db.ns1"));
        Assert.assertTrue(tblSet4.contains("db.metadata"));
    }
}
