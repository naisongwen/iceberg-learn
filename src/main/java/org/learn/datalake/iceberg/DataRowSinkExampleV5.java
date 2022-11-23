package org.learn.datalake.iceberg;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.learn.datalake.common.BoundedTestSource;
import org.learn.datalake.common.ExampleBase;
import org.learn.datalake.common.SimpleDataUtil;

import java.util.List;

public class DataRowSinkExampleV5 extends ExampleBase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        env.setParallelism(1);
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

        List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
                // Checkpoint #1
                ImmutableList.of(
                        row("+I", 1,  "aaa"),
                        row("-U", 1,  "aaa"),
                        row("+U", 1,  "bbb")
                ),
                // Checkpoint #2
                ImmutableList.of(
                        row("+I", 2, "ccc"),
                        row("-D", 1, "bbb")
                )
//                // Checkpoint #3
//                ImmutableList.of(
//                        row("-D", 1, "aaa"),
//                        row("+I", 1, "aaa")
//                ),
//                // Checkpoint #4
//                ImmutableList.of(
//                        row("-U", 1, "aaa"),
//                        row("+U", 1, "aaa"),
//                        row("+I", 1, "aaa")
//                )
        );

        DataStream<RowData> dataStream = env.addSource(new BoundedTestSource<>(
                        elementsPerCheckpoint
                ), ROW_TYPE_INFO)
                .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

//        DataStream<RowData> dataStream = env.addSource(new BoundedTestSource<>(
//                Row.of(RowKind.INSERT,1, "hello"),
//                Row.of(RowKind.DELETE,1, "world"),
//                Row.of(RowKind.INSERT,3, "foo")), ROW_TYPE_INFO)
//                .map(CONVERTER::toInternal, RowDataTypeInfo.of(SimpleDataUtil.ROW_TYPE));


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        tEnv.createTemporaryView("tbl",dataStream,"id,data");
        org.apache.flink.table.api.Table table=tEnv.sqlQuery("select id,count(*) from tbl group by id");
        DataStream<Tuple2<Boolean, Row>> retractStreamTable = tEnv.toRetractStream(table, Row.class);
        retractStreamTable.print();
//        try (CloseableIterator<Row> iter = result.collect()) {
//            ArrayList list=Lists.newArrayList(iter);
//            System.out.println(list);
//        }
        env.execute("Test Iceberg DataStream");
    }
}
