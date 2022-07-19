package org.learn.datalake.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
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
import org.learn.datalake.common.BoundedTestSourceWithoutCK;
import org.learn.datalake.common.ExampleBase;
import org.learn.datalake.common.SimpleDataUtil;

import java.io.File;

import static org.learn.datalake.common.TableTestBase.getTableOrCreate;

public class DataRowSinkExampleV4 extends ExampleBase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
                SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

        DataStream<RowData> dataStream = env.addSource(new BoundedTestSourceWithoutCK<>(
                        row("+I", 1, "aaa"),
                        row("+U", 1, "bbb")
                ), ROW_TYPE_INFO)
//                .setParallelism(1)//NOTE:ensure the order
                .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE))
                .setParallelism(1);

        File warehouse = new File("warehouse/test_sink_V4");
        Table table = getTableOrCreate(warehouse, true);
        //dataStream = dataStream.keyBy((KeySelector) value -> ((RowData)value).getInt(0));

        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        TableLoader tableLoader = TableLoader.fromHadoopTable(warehouse.getAbsolutePath());
        FlinkSink.forRowData(dataStream)
                .table(table)
                .tableLoader(tableLoader)
                .equalityFieldColumns(ImmutableList.of("id"))
                .writeParallelism(1)
                .distributionMode(DistributionMode.HASH)
                .build();

        env.execute("Test Iceberg DataStream");
        table.refresh();
        CloseableIterable<Record> iterable = IcebergGenerics.read(table).build();
        String data = Iterables.toString(iterable);
        System.out.println(data);
    }
}
