package org.learn.datalake.dataview;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.json.JSONObject;
import org.junit.rules.TemporaryFolder;
import org.learn.datalake.common.BoundedTestSource;
import org.learn.datalake.common.SimpleDataUtil;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class DataRowSinkHdfsExample {
    public static final TableSchema FLINK_SCHEMA_JSON = TableSchema.builder()
            .field("name", DataTypes.INT())
            .field("company", DataTypes.STRING())
            .field("position", DataTypes.STRING())
            .build();
    public static void main(String[] args) {
        List<Row> rows = Lists.newArrayList(
                Row.of("阿飞", "deepexi","java"),
                Row.of("番茄", "deepexi","java"),
                Row.of("双休", "deepexi","java")
        );
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .enableCheckpointing(100)
                .setParallelism(1)
                .setMaxParallelism(1);
        TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
                FLINK_SCHEMA_JSON.getFieldTypes());

        DataFormatConverters.RowConverter CONVERTER = new DataFormatConverters.RowConverter(
                FLINK_SCHEMA_JSON.getFieldDataTypes());

        DataStream<RowData> dataStream = env.addSource(new BoundedTestSource<>(rows.toArray(new Row[0])), ROW_TYPE_INFO)
                .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

        String tablePath=null;
        try {
            TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder(new File("target"));
            TEMPORARY_FOLDER.create();

            File folder = TEMPORARY_FOLDER.newFolder();
            String warehouse = folder.getAbsolutePath();
            tablePath = warehouse.concat("\\test");
        }catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }

        FileFormat format=FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
        // Map<String, String> props=new HashMap<>();
        // props.put("fs.default-scheme","hdfs://10.201.0.212:8020");
        Table table = SimpleDataUtil.createTable(tablePath, props, false);

        try {
            TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);
            FlinkSink.forRowData(dataStream)
                    .table(table)
                    .tableLoader(tableLoader)
                    .writeParallelism(1)
                    .build();
            env.execute("Test Iceberg DataStream");
        }catch (Exception e){
            e.printStackTrace();
        }

        Table distTable=new HadoopTables().load(tablePath);
        distTable.refresh();

        try (CloseableIterable<Record> iterable = IcebergGenerics.read(distTable).build()){
            String data= Iterables.toString(iterable);
            System.out.println(data);
        }catch (Exception e){
            e.printStackTrace();
        }
    }




}
