package org.learn.datalake.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
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

public class DataRowSinkExampleV3 extends ExampleBase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(100L);
//        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000L);
//        checkpointConfig.setTolerableCheckpointFailureNumber(10);
//        checkpointConfig.setCheckpointTimeout(120 * 1000L);
//        checkpointConfig.enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


//        TypeInformation<RowData> ROW_TYPE_INFO = new RowDataTypeInfo(
//                SimpleDataUtil.FLINK_SCHEMA.toPhysicalRowDataType().getLogicalType());
        TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
                SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

        DataStream<RowData> dataStream = env.addSource(new BoundedTestSource<>(
                row("+I", 1, "aaa"),
                row("-D", 1, "aaa"),
                row("+I", 3, "ccc")), ROW_TYPE_INFO)
                .map(CONVERTER::toInternal, RowDataTypeInfo.of(SimpleDataUtil.ROW_TYPE));

//        DataStream<RowData> dataStream = env.addSource(new BoundedTestSource<>(
//                Row.of(RowKind.INSERT,1, "hello"),
//                Row.of(RowKind.DELETE,1, "world"),
//                Row.of(RowKind.INSERT,3, "foo")), ROW_TYPE_INFO)
//                .map(CONVERTER::toInternal, RowDataTypeInfo.of(SimpleDataUtil.ROW_TYPE));


        TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder(new File("target"));
        TEMPORARY_FOLDER.create();
        File folder = TEMPORARY_FOLDER.newFolder();
        String warehouse = folder.getAbsolutePath();
        String tablePath = warehouse.concat("/test");

        //dataStream = dataStream.keyBy((KeySelector) value -> ((RowData)value).getInt(ROW_ID_POS));
        // need to upgrade version to 2,otherwise 'java.lang.IllegalArgumentException: Cannot write
        // delete files in a v1 table'

        FileFormat format=FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
        Table table = SimpleDataUtil.createTable(tablePath, props, false);

        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);
        FlinkSink.forRowData(dataStream)
                .table(table)
                .tableLoader(tableLoader)
                .equalityFieldColumns(Arrays.asList("id"))
                .writeParallelism(1)
                .build();


        env.execute("Test Iceberg DataStream");

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
