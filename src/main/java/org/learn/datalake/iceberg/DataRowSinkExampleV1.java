package org.learn.datalake.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.learn.datalake.common.SimpleDataUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.learn.datalake.common.BoundedTestSource;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.Locale;
import java.util.Map;

//https://github.com/ververica/flink-cdc-connectors

public class DataRowSinkExampleV1 {

    public static void main(String[] args) throws Exception {
        List<Row> rows = Lists.newArrayList(
                Row.of(1, "hello"),
                Row.of(2, "world"),
                Row.of(3, "foo")
        );


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .enableCheckpointing(100)
                .setParallelism(1)
                .setMaxParallelism(1);

        TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
                SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

        DataFormatConverters.RowConverter CONVERTER = new DataFormatConverters.RowConverter(
                SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());

        DataStream<RowData> dataStream = env.addSource(new BoundedTestSource<>(rows.toArray(new Row[0])), ROW_TYPE_INFO)
                .map(CONVERTER::toInternal, RowDataTypeInfo.of(SimpleDataUtil.ROW_TYPE));

        TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder(new File("target"));
        TEMPORARY_FOLDER.create();
        File folder = TEMPORARY_FOLDER.newFolder();
        String warehouse = folder.getAbsolutePath();
        String tablePath = warehouse.concat("/test");

        FileFormat format=FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
        Table table = SimpleDataUtil.createTable(tablePath, props, false);

        TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);
        FlinkSink.forRowData(dataStream)
                .table(table)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .build();
        env.execute("Test Iceberg DataStream");

        Table distTable=new HadoopTables().load(tablePath);
        distTable.refresh();
        try (CloseableIterable<Record> iterable = IcebergGenerics.read(distTable).build()){
            String data=Iterables.toString(iterable);
            System.out.println(data);
        }
    }
}
