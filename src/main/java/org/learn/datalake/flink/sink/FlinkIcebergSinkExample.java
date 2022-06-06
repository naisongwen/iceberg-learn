package org.learn.datalake.flink.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.*;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.learn.datalake.common.BoundedTestSource;
import org.learn.datalake.common.ExampleBase;
import org.learn.datalake.common.SimpleDataUtil;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.learn.datalake.common.TableTestBase.getTableOrCreate;


public class FlinkIcebergSinkExample extends ExampleBase {

    protected static DataFormatConverters.RowConverter CONVERTER = new DataFormatConverters.RowConverter(
            SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());

    protected static final Map<String, RowKind> ROW_KIND_MAP = ImmutableMap.of(
            "+I", RowKind.INSERT,
            "-D", RowKind.DELETE,
            "-U", RowKind.UPDATE_BEFORE,
            "+U", RowKind.UPDATE_AFTER);

    protected static Row row(String rowKind, int id, String data) {
        RowKind kind = ROW_KIND_MAP.get(rowKind);
        if (kind == null) {
            throw new IllegalArgumentException("Unknown row kind: " + rowKind);
        }

        return Row.ofKind(kind, id, data);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        checkpointConfig.setCheckpointInterval(100L);

        TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
                SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

        RowType rowType = FlinkSchemaUtil.convert(SimpleDataUtil.SCHEMA);

        final Schema SCHEMA2 = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get()),
                Types.NestedField.optional(3, "name", Types.StringType.get())
        );

//        DataStream<RowData> dataStream = env.fromCollection(
//                Lists.newArrayList(
//                        row("+I", 1, "aaa"),
//                        row("-D", 1, "aaa"),
//                        row("+I", 2, "bbb")
//                ),ROW_TYPE_INFO).map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(rowType));

        DataStream<GenericRowDataWithSchema> dataStream = env.fromCollection(
                Lists.newArrayList(
                        GenericRowDataWithSchema.of(1, StringData.fromString("a")),
                    GenericRowDataWithSchema.of(SCHEMA2,2,StringData.fromString("b"),StringData.fromString("b2"))
                ));

        File warehouse = new File("warehouse/default/test_flink_sink2");
        Table table = getTableOrCreate(warehouse,SimpleDataUtil.SCHEMA,true);

        //dataStream = dataStream.keyBy((KeySelector) value -> ((RowData)value).getInt(0));
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        TableLoader tableLoader = TableLoader.fromHadoopTable(warehouse.getAbsolutePath());
        FlinkSink.forRowData(dataStream).tableLoader(tableLoader).table(table).build();
        env.execute();

        table.refresh();
        printTableData(table);
    }
}
