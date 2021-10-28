package org.learn.datalake.iceberg;

import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
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
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.learn.datalake.common.BoundedTestSource;
import org.learn.datalake.common.ExampleBase;
import org.learn.datalake.common.SimpleDataUtil;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

//Reference iceberg HadoopTableTestBase
public class CatalogSinkExample extends ExampleBase {
    //--warehouse warehouse/hive_db/hive_table -hive_db hive_db --hive_table hive_table --catalog_type hive
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(100L);
//        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000L);
//        checkpointConfig.setTolerableCheckpointFailureNumber(10);
//        checkpointConfig.setCheckpointTimeout(120 * 1000L);
//        checkpointConfig.enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
                SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

        RowType rowType = FlinkSchemaUtil.convert(SimpleDataUtil.SCHEMA);

        DataStream<RowData> dataStream = env.addSource(new BoundedTestSource<>(
                        row("+I", 1, "aaa"),
                        row("+U", 1, "AAA")
                ), ROW_TYPE_INFO)
                .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(rowType));

        //https://iceberg.apache.org/flink/#hive-catalog
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("property-version", "1");
        File warehouse = new File(parameterTool.get("warehouse"));
        if (warehouse.exists())
            FileUtils.cleanDirectory(warehouse);
        warehouse.mkdirs();
        String warehouseDir = warehouse.getAbsolutePath();
        properties.put("warehouse", parameterTool.get("warehouse"));
        String catalogType = parameterTool.get("catalog_type", "hadoop");
        CatalogLoader catalogLoader = null;
        if ("hadoop".equals(catalogType)) {
            properties.put("catalog-type", "hadoop");
            String HADOOP_CATALOG = "iceberg_hadoop_catalog";
            catalogLoader =
                    CatalogLoader.hadoop(HADOOP_CATALOG, new Configuration(), properties);
        } else if ("hive".equals(catalogType)) {
            properties.put("catalog-type", "hive");
            //WARNING: setup HiveMetaStoreServer in project hive-learn
            String thriftUri = String.format("thrift://localhost:%s", parameterTool.get("port", "51846"));
            properties.put("uri", thriftUri);
            String HIVE_CATALOG = "iceberg_hive_catalog";
            catalogLoader = CatalogLoader.hive(HIVE_CATALOG, new Configuration(), properties);

            HiveConf hiveConf = new HiveConf(new Configuration(), CatalogSinkExample.class);

            hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUri);
            hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:" + warehouseDir);
            HiveCatalog catalog = new HiveCatalog(hiveConf);
            HiveMetaStoreClient metastoreClient = new HiveMetaStoreClient(hiveConf);

            String dbPath = new File(warehouseDir, parameterTool.get("hive_db") + ".db").getPath();
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
                    catalog.buildTable(tableIdentifier, SimpleDataUtil.SCHEMA)
                            .withPartitionSpec(PartitionSpec.unpartitioned())
                            .create();
        }
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
        dataStream = dataStream.keyBy((KeySelector) value -> ((RowData) value).getInt(0));
        FlinkSink.forRowData(dataStream)
                .table(table)
                .tableLoader(tableLoader)
                .equalityFieldColumns(ImmutableList.of("id"))//Arrays.asList("id"))
                .build();

        env.execute();

        table.refresh();
//        for (Snapshot snapshot : table.snapshots()) {
//            long snapshotId = snapshot.snapshotId();
//            try (CloseableIterable<Record> reader = IcebergGenerics.read(table)
//                    .useSnapshot(snapshotId)
//                    .select("*")
//                    .build()) {
//                reader.forEach(System.out::print);
//            }
//        }
//        System.out.println();
        try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            String data = Iterables.toString(iterable);
            System.out.println(data);
        }
    }
}
