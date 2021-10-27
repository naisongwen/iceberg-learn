package org.learn.datalake.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.*;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.rules.TemporaryFolder;
import org.learn.datalake.common.BoundedTestSource;
import org.learn.datalake.common.ExampleBase;
import org.learn.datalake.common.SimpleDataUtil;

import java.io.File;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import static org.learn.datalake.metadata.TableTestBase.getTableOrCreate;

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
                        row("-D", 1, "aaa")
                ), ROW_TYPE_INFO)
                .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

//        DataStream<RowData> dataStream = env.addSource(new BoundedTestSource<>(
//                Row.of(RowKind.INSERT,1, "hello"),
//                Row.of(RowKind.DELETE,1, "world"),
//                Row.of(RowKind.INSERT,3, "foo")), ROW_TYPE_INFO)
//                .map(CONVERTER::toInternal, RowDataTypeInfo.of(SimpleDataUtil.ROW_TYPE));


        File warehouse = new File("warehouse/test_upsert_file_V3");
        Table table = getTableOrCreate(warehouse,true);

        dataStream = dataStream.keyBy((KeySelector) value -> ((RowData)value).getInt(0));
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        TableLoader tableLoader = TableLoader.fromHadoopTable(warehouse.getAbsolutePath());
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
    }
}
