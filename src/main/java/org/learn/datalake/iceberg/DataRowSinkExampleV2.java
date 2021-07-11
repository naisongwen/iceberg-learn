package org.learn.datalake.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.iceberg.*;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.rules.TemporaryFolder;
import org.learn.datalake.common.ExampleBase;
import org.learn.datalake.common.SimpleDataUtil;
import org.learn.datalake.common.BoundedTestSource;

import java.io.File;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

//https://github.com/ververica/flink-cdc-connectors

public class DataRowSinkExampleV2 extends ExampleBase {

    public static void main(String[] args) throws Exception {
        TableSchema schema =
                TableSchema.builder()
                        .add(TableColumn.of("id", DataTypes.INT()))
                        .add(TableColumn.of("data", DataTypes.STRING()))
                        .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
                SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

        DataStream<Row> dataStream = env.addSource(new BoundedTestSource<>(
                        row("+I", 1, "aaa"),
                        row("-D", 1, "bbb"),
                        row("+I", 3, "ccc")),
                ROW_TYPE_INFO);
        //env.getConfig().disableGenericTypes();
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        checkpointConfig.setCheckpointInterval(5 * 1000L);
//        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000L);
//        checkpointConfig.setTolerableCheckpointFailureNumber(10);
//        checkpointConfig.setCheckpointTimeout(120 * 1000L);
//        checkpointConfig.enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder(new File("target"));
        TEMPORARY_FOLDER.create();
        File folder = TEMPORARY_FOLDER.newFolder();
        String warehouse = folder.getAbsolutePath();
        String tablePath = warehouse.concat("/test");

        FileFormat format = FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
        Table table = SimpleDataUtil.createTable(tablePath, props, false);


        TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);

        // need to upgrade version to 2,otherwise 'java.lang.IllegalArgumentException: Cannot write
        // delete files in a v1 table'
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        FlinkSink.forRow(dataStream, schema)
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

        try (
                CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            String data = Iterables.toString(iterable);
            System.out.println(data);
        }
    }
}
