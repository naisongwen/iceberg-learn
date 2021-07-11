//package org.learn.datalake.catalog;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.iceberg.*;
//import org.apache.iceberg.catalog.TableIdentifier;
//import org.apache.iceberg.types.Types;
//import org.junit.Assert;
//import org.junit.Rule;
//import org.junit.rules.TemporaryFolder;
//
//import java.io.IOException;
//
//import static org.apache.iceberg.types.Types.NestedField.required;
//
//public class JdbcCatalogExample {
//    static final Schema SCHEMA = new Schema(
//            required(3, "id", Types.IntegerType.get(), "unique ID"),
//            required(4, "data", Types.StringType.get())
//    );
//
//    static final PartitionSpec PARTITION_SPEC = PartitionSpec.builderFor(SCHEMA)
//            .bucket("data", 16)
//            .build();
//
//    static Configuration conf = new Configuration();
//    private static JdbcCatalog catalog;
//    private static String warehouseLocation;
//
//    @Rule
//    public TemporaryFolder temp = new TemporaryFolder();
//
//    public static void main(String[] args) throws IOException {
//        TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
//
//        final DataFile fileA = DataFiles.builder(PARTITION_SPEC)
//                .withPath("/path/to/data-a.parquet")
//                .withFileSizeInBytes(0)
//                .withPartitionPath("data_bucket=0") // easy way to set partition data for now
//                .withRecordCount(2) // needs at least one record or else metrics will filter it out
//                .build();
//
//        Transaction createTxn = catalog.buildTable(tableIdent, SCHEMA)
//                .withPartitionSpec(PARTITION_SPEC)
//                .withProperty("key1", "value1")
//                .createOrReplaceTransaction();
//
//        createTxn.newAppend()
//                .appendFile(fileA)
//                .commit();
//
//        createTxn.commitTransaction();
//
//        Table table = catalog.loadTable(tableIdent);
//        Assert.assertNotNull(table.currentSnapshot());
//
//        Transaction replaceTxn = catalog.buildTable(tableIdent, SCHEMA)
//                .withProperty("key2", "value2")
//                .replaceTransaction();
//        replaceTxn.commitTransaction();
//
//        table = catalog.loadTable(tableIdent);
//        Assert.assertNull(table.currentSnapshot());
//        Assert.assertTrue(table.spec().isUnpartitioned());
//        Assert.assertEquals("value1", table.properties().get("key1"));
//        Assert.assertEquals("value2", table.properties().get("key2"));
//    }
//}
