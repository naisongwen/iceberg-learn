package org.learn.datalake.catalog;

import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.learn.datalake.common.BoundedTestSource;
import org.learn.datalake.common.ExampleBase;
import org.learn.datalake.common.SimpleDataUtil;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//from iceberg HadoopTableTestBase
public class CatalogSinkExample extends ExampleBase {

    private static final Schema tableSchema =
            new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "data", Types.StringType.get())
            );

    //--warehouse target/hive_db/hive_table -hive_db hive_db --hive_table hive_table --catalog_type hive
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        checkpointConfig.setCheckpointInterval(100L);
//        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000L);
//        checkpointConfig.setTolerableCheckpointFailureNumber(10);
//        checkpointConfig.setCheckpointTimeout(120 * 1000L);
//        checkpointConfig.enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
                SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

        RowType rowType = FlinkSchemaUtil.convert(tableSchema);

        DataStream<RowData> dataStream = env.addSource(new BoundedTestSource<>(
                        row("+I", 1, "aaa"),
                        row("-D", 1, "aaa"),
                        row("+I", 3, "ccc")), ROW_TYPE_INFO)
                .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(rowType));

        //https://iceberg.apache.org/flink/#hive-catalog
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("property-version", "1");
        properties.put("warehouse", parameterTool.get("warehouse"));

        String catalogType = parameterTool.get("catalog_type");
        CatalogLoader catalogLoader = null;
        if ("hadoop".equals(catalogType)) {
            properties.put("catalog-type", "hadoop");
            String HADOOP_CATALOG = "iceberg_hadoop_catalog";
            catalogLoader =
                    CatalogLoader.hadoop(HADOOP_CATALOG, new Configuration(), properties);
        } else if ("hive".equals(catalogType)) {
            properties.put("catalog-type", "hive");
            //start hive metaserver first in another app
            properties.put("uri", "thrift://localhost:58883");
            String HIVE_CATALOG = "iceberg_hive_catalog";
            catalogLoader = CatalogLoader.hive(HIVE_CATALOG, new Configuration(), properties);

            HiveConf hiveConf = new HiveConf(new Configuration(), CatalogSinkExample.class);
            hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + 58883);
            // in Hive3, setting this as a system prop ensures that it will be picked up whenever a new HiveConf is created
            File warehouse = new File("warehouse");
            if (warehouse.exists())
                FileUtils.deleteDirectory(warehouse);
            String hiveLocalDir = warehouse.getAbsolutePath();
            hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:" + hiveLocalDir);
            HiveCatalog catalog = new HiveCatalog(hiveConf);
            HiveMetaStoreClient metastoreClient = new HiveMetaStoreClient(hiveConf);

            String dbPath = new File(hiveLocalDir, parameterTool.get("hive_db") + ".db").getPath();
            Database db = new Database(parameterTool.get("hive_db"), "description", dbPath, new HashMap<>());
            try {
                metastoreClient.createDatabase(db);
            } catch (Exception e) {
            }
        } else {
            throw new IllegalArgumentException("catalog type must not be empty");
        }

        Catalog catalog = catalogLoader.loadCatalog();

        TableIdentifier tableIdentifier =
                TableIdentifier.of(Namespace.of(parameterTool.get("hive_db")), parameterTool.get("hive_table"));

        Table table;
        if (catalog.tableExists(tableIdentifier)) {
            table = catalog.loadTable(tableIdentifier);
        } else {
            table =
                    catalog.buildTable(tableIdentifier, tableSchema)
                            .withPartitionSpec(PartitionSpec.unpartitioned())
                            .create();
        }
        // need to upgrade version to 2,otherwise 'java.lang.IllegalArgumentException: Cannot write
        // delete files in a v1 table'
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

        FlinkSink.forRowData(dataStream)
                .table(table)
                .tableLoader(tableLoader)
                .equalityFieldColumns(Arrays.asList("id"))
                .writeParallelism(1)
                .build();

        env.execute();

        table.refresh();
        for (Snapshot snapshot : table.snapshots()) {
            long snapshotId = snapshot.snapshotId();
            try (CloseableIterable<Record> reader = IcebergGenerics.read(table)
                    .useSnapshot(snapshotId)
                    .select("*")
                    .build()) {
                reader.forEach(System.out::print);
            }
        }
        System.out.println();
        try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            String data = Iterables.toString(iterable);
            System.out.println(data);
        }
    }
}
