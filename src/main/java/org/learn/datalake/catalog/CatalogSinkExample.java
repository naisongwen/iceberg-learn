package org.learn.datalake.catalog;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.learn.datalake.common.SimpleDataUtil;
import org.learn.datalake.common.ExampleBase;
import org.learn.datalake.common.BoundedTestSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CatalogSinkExample extends ExampleBase {
    private static final Schema SCHEMA =
            new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "data", Types.StringType.get())
            );

    private static final String HIVE_CATALOG = "iceberg_hive_catalog";
    private static final String HADOOP_CATALOG = "iceberg_hadoop_catalog";

    //--warehouse target/hive_db/hive_table -hive_db hive_db --hive_table hive_table

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(100L);
//        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000L);
//        checkpointConfig.setTolerableCheckpointFailureNumber(10);
//        checkpointConfig.setCheckpointTimeout(120 * 1000L);
//        checkpointConfig.enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
                SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

        DataStream<RowData> dataStream = env.addSource(new BoundedTestSource<>(
                row("+I", 1, "aaa"),
                row("-D", 1, "aaa"),
                row("+I", 3, "ccc")), ROW_TYPE_INFO)
                .map(CONVERTER::toInternal, RowDataTypeInfo.of(SimpleDataUtil.ROW_TYPE));


        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("property-version", "1");
        properties.put("warehouse", parameterTool.get("warehouse"));
        properties.put("catalog-type", "hadoop");
        //properties.put("catalog-type", "hive");
        //CatalogLoader catalogLoader =CatalogLoader.hive(HIVE_CATALOG, new Configuration(), properties);

        CatalogLoader catalogLoader =
                CatalogLoader.hadoop(HADOOP_CATALOG, new Configuration(), properties);

        Catalog catalog = catalogLoader.loadCatalog();

        TableIdentifier identifier =
                TableIdentifier.of(Namespace.of(parameterTool.get("hive_db")), parameterTool.get("hive_table"));

        Table table;
        if (catalog.tableExists(identifier)) {
            table = catalog.loadTable(identifier);
        } else {
            table =
                    catalog.buildTable(identifier, SCHEMA)
                            .withPartitionSpec(PartitionSpec.unpartitioned())
                            .create();
        }
        // need to upgrade version to 2,otherwise 'java.lang.IllegalArgumentException: Cannot write
        // delete files in a v1 table'
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

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
        try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()){
            String data=Iterables.toString(iterable);
            System.out.println(data);
        }
    }
}
