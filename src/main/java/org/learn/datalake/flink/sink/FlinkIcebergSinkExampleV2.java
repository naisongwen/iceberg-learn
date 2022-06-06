package org.learn.datalake.flink.sink;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.learn.datalake.common.ExampleBase;
import org.learn.datalake.common.SimpleDataUtil;
import org.learn.datalake.common.UnBoundedTestSource;

import static org.learn.datalake.common.TableTestBase.getTableOrCreate;

public class FlinkIcebergSinkExampleV2 extends ExampleBase {

  protected static final Map<String, RowKind> ROW_KIND_MAP = ImmutableMap.of(
      "+I", RowKind.INSERT,
      "-D", RowKind.DELETE,
      "-U", RowKind.UPDATE_BEFORE,
      "+U", RowKind.UPDATE_AFTER);
  protected static DataFormatConverters.RowConverter CONVERTER = new DataFormatConverters.RowConverter(
      SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());

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
    checkpointConfig.setCheckpointInterval(10000L);

    RowType rowType = FlinkSchemaUtil.convert(SimpleDataUtil.SCHEMA);

    final Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get())
    );

    List<GenericRowDataWithSchema> list = Lists.newArrayList(
        GenericRowDataWithSchema.of(schema, 1, StringData.fromString("a")),
        GenericRowDataWithSchema.of(schema, 2, StringData.fromString("b")),
        GenericRowDataWithSchema.of(schema, 3, StringData.fromString("c")),
        GenericRowDataWithSchema.of(schema, 4, StringData.fromString("d")),
        GenericRowDataWithSchema.of(schema, 5, StringData.fromString("e"))
    );

    DataStream<Record> dataStream = env.addSource(new UnBoundedTestSource(schema));
    File warehouse = new File("warehouse/default/test_flink_sink2/");
    env.setStateBackend(new FsStateBackend("file://"+new File("checkpoint-dir").getAbsolutePath()));
    //DataStream<GenericRowDataWithSchema> dataStream = env.fromCollection(list);

    ProcessWindowFunction processWindowFunction = new StatProcessWindowsFunction();
    SingleOutputStreamOperator<GenericRowDataWithSchema> wndStream =
        dataStream.keyBy(pr -> pr.get(0))
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) //设置时间窗口
            .process(processWindowFunction);

    final Schema targetSchema = new Schema(
        Types.NestedField.required(1, "col_name", Types.StringType.get()),
        Types.NestedField.required(2, "col_value", Types.StringType.get()),
        Types.NestedField.required(3, "col_count", Types.LongType.get()),
        Types.NestedField.required(4, "dt", Types.TimestampType.withoutZone())
    );
    final PartitionSpec spec = PartitionSpec.builderFor(targetSchema).identity("col_name")
        .hour("dt")
        .build();

    Table table = getTableOrCreate(warehouse, targetSchema, spec, true);

    //dataStream = dataStream.keyBy((KeySelector) value -> ((RowData)value).getInt(0));
    TableOperations operations = ((BaseTable) table).operations();
    TableMetadata metadata = operations.current();
    operations.commit(metadata, metadata.upgradeToFormatVersion(2));

    TableLoader tableLoader = TableLoader.fromHadoopTable(warehouse.getAbsolutePath());
    FlinkSink.forRowData(wndStream).tableLoader(tableLoader).table(table).build();
    env.execute();
  }

  private static class StatProcessWindowsFunction extends
      ProcessWindowFunction<Record, GenericRowDataWithSchema, Integer, TimeWindow> {
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    @Override
    public void process(
        Integer key,
        ProcessWindowFunction<Record, GenericRowDataWithSchema, Integer, TimeWindow>.Context context,
        Iterable<Record> elements,
        Collector<GenericRowDataWithSchema> out) {
      Map<String, Map<Object, Long>> mapBykey = Maps.newHashMap();
      elements.forEach(v -> {
        Record rowData = v;
        List<Types.NestedField> fields = rowData.struct().fields();
        for (int i = 0; i < fields.size(); i++) {
          Object value = rowData.get(i);
          if (value != null) {
            String name = fields.get(i).name();
            Map<Object, Long> countByKey = mapBykey.getOrDefault(name, Maps.newHashMap());
            countByKey.put(value, 1 + countByKey.getOrDefault(value, 0L));
            mapBykey.put(name,countByKey);
          }
        }
      });
      Long wndEndTime = context.window().getEnd();
      mapBykey.forEach((k, v) -> {
        GenericRowDataWithSchema genericRowDataWithSchema = new GenericRowDataWithSchema(4);
        genericRowDataWithSchema.setField(0, StringData.fromString(k));
        Map<Object, Long> countByKey = mapBykey.get(k);
        countByKey.forEach((k2, v2) -> {
          genericRowDataWithSchema.setField(1, StringData.fromString(k2.toString()));
          genericRowDataWithSchema.setField(2, v2);
          String dt = DATE_FORMAT.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(wndEndTime), ZoneOffset.UTC));
          genericRowDataWithSchema.setField(3, TimestampData.fromEpochMillis(wndEndTime));
          out.collect(genericRowDataWithSchema);
        });
      });
    }
  }
}
