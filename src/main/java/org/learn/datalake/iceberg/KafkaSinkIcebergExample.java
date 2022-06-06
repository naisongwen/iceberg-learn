package org.learn.datalake.iceberg;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.learn.datalake.common.*;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class KafkaSinkIcebergExample extends TableTestBase {

    private static Map<String, Type> typeByName = new LinkedHashMap() {
        {
            put("successEventCount", Types.LongType.get());
            put("failEventCount", Types.LongType.get());
            put("statPeriodBegin", Types.TimestampType.withoutZone());
            put("statPeriodEnd", Types.TimestampType.withoutZone());
        }
    };

    private static Map<String, DataType> dataTypeByName = new LinkedHashMap() {
        {
            put("successEventCount", DataTypes.BIGINT());
            put("failEventCount", DataTypes.BIGINT());
            put("statPeriodBegin", DataTypes.TIMESTAMP());
            put("statPeriodEnd", DataTypes.TIMESTAMP());
        }
    };

    private static Schema getSchema(ParseResult parseResult) {
        Map<String, Object> keyValuesMap = parseResult.getKeyValuesMap();
        List<Types.NestedField> nestedFieldList = Lists.newArrayList();
        int index = 0;
        for (Map.Entry<String, Object> entry : keyValuesMap.entrySet()) {
            Object object = entry.getValue();
            Type type = Types.StringType.get();
            if (object instanceof Integer)
                type = Types.IntegerType.get();
            else if (object instanceof Long)
                type = Types.LongType.get();
            else if (object instanceof Boolean)
                type = Types.BooleanType.get();
            else if (object instanceof Double)
                type = Types.DoubleType.get();

            nestedFieldList.add(Types.NestedField.optional(++index, entry.getKey(), type));
        }
        Schema schema = new Schema(nestedFieldList);
        return schema;
    }

    private static TableSchema getTableSchema(Schema schema) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        List<Types.NestedField> fieldList = schema.columns();
        for (Types.NestedField field : fieldList) {
            Type type = field.type();
            DataType dataType = DataTypes.STRING();
            if (type.asPrimitiveType().typeId() == Type.TypeID.INTEGER)
                dataType = DataTypes.INT();
            else if (type.asPrimitiveType().typeId() == Type.TypeID.LONG)
                dataType = DataTypes.BIGINT();
            else if (type.asPrimitiveType().typeId() == Type.TypeID.BOOLEAN)
                dataType = DataTypes.BOOLEAN();
            else if (type.asPrimitiveType().typeId() == Type.TypeID.DOUBLE)
                dataType = DataTypes.DOUBLE();

            schemaBuilder.field(field.name(), dataType);
        }
        return schemaBuilder.build();
    }

    private static Schema getStatSchema(Schema schema) {
        List<Types.NestedField> nestedFieldList = Lists.newArrayList();
        typeByName.forEach((k, v) -> {
            nestedFieldList.add(Types.NestedField.optional(nestedFieldList.size(), k, v));
        });
        int index = nestedFieldList.size();
        List<Types.NestedField> fieldList = schema.columns();
        for (Types.NestedField field : fieldList) {
            nestedFieldList.add(Types.NestedField.optional(++index, field.name(),
                    Types.MapType.ofOptional(++index, ++index, Types.StringType.get(), Types.StringType.get()))
            );
//            nestedFieldList.add(Types.NestedField.optional(++index, field.name(), Types.StructType.of(
//                    Types.NestedField.optional(++index, "valueCount", Types.LongType.get()),
//                    Types.NestedField.optional(++index, "valueType", Types.StringType.get()),
//                    Types.NestedField.optional(++index, "maxValue", Types.DoubleType.get()),
//                    Types.NestedField.optional(++index, "minValue", Types.DoubleType.get()))));
        }
        Schema statSchema = new Schema(nestedFieldList);
        return statSchema;
    }

    private static TableSchema getStatTableSchema(Schema schema) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        List<Types.NestedField> fieldList = schema.columns();
        for (Map.Entry<String,DataType> entry: dataTypeByName.entrySet()) {
            schemaBuilder.field(entry.getKey(),entry.getValue());
        }
        for (Types.NestedField field : fieldList) {
            DataType dataType = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
//                    StatMetrics.class,
//                    DataTypes.FIELD("valueCount", DataTypes.BIGINT()),
//                    DataTypes.FIELD("valueType", DataTypes.STRING()),
//                    DataTypes.FIELD("maxValue", DataTypes.DOUBLE()),
//                    DataTypes.FIELD("minValue", DataTypes.DOUBLE()
//                    )
//            );
            schemaBuilder.field(field.name(), dataType);
        }
        return schemaBuilder.build();
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", Constants.kafkaServers);
        standardProps.put("group.id", "kafkaWriter-tests");
        standardProps.put("enable.auto.commit", false);
        standardProps.put("key.serializer", ByteArraySerializer.class.getName());
        standardProps.put("value.serializer", ByteArraySerializer.class.getName());
        standardProps.put("auto.offset.reset", "earliest");
        return standardProps;
    }

    public static void main(String[] args) throws Exception {
        final String topic = "sample_topic";
        final int winSize = 1;
        // ---------- Produce an event time stream into Kafka -------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new FsStateBackend("file://" + new File("checkpoint-dir").getAbsolutePath()));
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.getCheckpointConfig().setCheckpointInterval(6*1000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        Deserializer<String> deserializer = new StringDeserializer();
        //默认提供了 KafkaDeserializationSchema(序列化需要自己编写)、JsonDeserializationSchema、AvroDeserializationSchema、TypeInformationSerializationSchema
        KafkaRecordDeserializer recordDeserializer = new TestingKafkaRecordDeserializer();

        List<ConsumerRecord<byte[], byte[]>> records = KafkaUtil.withDrawNumberedRecordsFromTopic(topic, getKafkaClientConfiguration(), 1, true);

        Schema dataSchema = null;
        Schema statSchema = null;
        TableSchema dataTableSchema = null;
        TableSchema statTableSchema = null;
        String dataWarehouse = new File("warehouse/default/kafka_iceberg_data").getAbsolutePath();
        String statWarehouse = new File("warehouse/default/kafka_iceberg_stat").getAbsolutePath();

        Preconditions.checkState(records.size() > 0,"No data fetched from kafka");
        ConsumerRecord<byte[], byte[]> record = records.get(0);
        MetaAndValue metaAndValue = new MetaAndValue(
                new TopicPartition(record.topic(), record.partition()),
                deserializer.deserialize(record.topic(), record.value()), record.offset());
        ParseResult parseResult = ParseResult.parse(metaAndValue);
        if (parseResult.isParseSuccess()) {
            dataSchema = getSchema(parseResult);
            ParseResult.setSchema(dataSchema);
            dataTableSchema = getTableSchema(dataSchema);
            getOrCreateHadoopTable(dataWarehouse, dataSchema, true);

            statSchema = getStatSchema(dataSchema);
            statTableSchema = getStatTableSchema(dataSchema);
            getOrCreateHadoopTable(statWarehouse, statSchema, true);

            // Convert the flink schema to iceberg schema firstly, then reassign ids to match the existing iceberg schema.
//            Schema writeSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(statTableSchema), statSchema);
//            TypeUtil.validateWriteSchema(statSchema, writeSchema, true, true);
        }

        Properties properties = new Properties();
//        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200);

        KafkaSource<MetaAndValue> kafkaSource =
                KafkaSource.<ObjectNode>builder()
                        .setBootstrapServers(Constants.kafkaServers)
                        .setGroupId(KafkaSinkIcebergExample.class.getName())
                        .setTopics(topic)
                        .setDeserializer(recordDeserializer)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setBounded(OffsetsInitializer.latest())
                        .setProperties(properties)
                        .build();

        //多分区，如果某分区拉取完成会导致CK无法完成
        DataStream<MetaAndValue> stream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(), "Read Kafka").setParallelism(1);

        DataStream<ParseResult> parseResultStream = stream.transform("Transform Raw String TO Structured Data", TypeInformation.of(ParseResult.class), new ParseResultStreamOperator()).rebalance();
        ProcessWindowFunction processWindowFunction = new StatProcessWindowsFunction();
        SingleOutputStreamOperator<Tuple2<Map<String, Object>, Iterable<ParseResult>>> wndStream = parseResultStream.keyBy(pr->pr.getProcessingTime()%1000)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1))) //设置时间窗口
                .process(processWindowFunction).setParallelism(winSize);

        SingleOutputStreamOperator<Row> dataStream = wndStream.transform("Transform Structured Data To Row", TypeInformation.of(Row.class), new DataRowStreamOperator());

        MergeStatProcessAllWindowFunction mergeStatProcessAllWindowFunction = new MergeStatProcessAllWindowFunction();
        SingleOutputStreamOperator<Row> statStream = wndStream.map(new MapFunction<Tuple2<Map<String, Object>, Iterable<ParseResult>>, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(Tuple2<Map<String, Object>, Iterable<ParseResult>> value) {
                return (Map<String, Object>) value.f0;
            }
        }).name("Map Structured Data To Stat Metrics").countWindowAll(winSize).process(mergeStatProcessAllWindowFunction).name("Merge Stat Metrics Into Single Row");

        TableLoader dataTableLoader = TableLoader.fromHadoopTable(dataWarehouse);
        FlinkSink.forRow(dataStream, dataTableSchema)
                .tableLoader(dataTableLoader)
                .tableSchema(dataTableSchema)
                .build();

        TableLoader statTableLoader = TableLoader.fromHadoopTable(statWarehouse);
        FlinkSink.forRow(statStream, statTableSchema)
                .tableLoader(statTableLoader)
                .tableSchema(statTableSchema)
                .build();

        env.execute();
    }

    private static class ParseResultStreamOperator extends AbstractStreamOperator<ParseResult>
            implements OneInputStreamOperator<MetaAndValue, ParseResult> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(StreamRecord<MetaAndValue> element) {
            RuntimeContext runtimeContext=getRuntimeContext();
            ParseResult parseResult = ParseResult.parse(element.getValue());
            output.collect(new StreamRecord(parseResult, parseResult.getProcessingTime()));
        }
    }


    private static class StatProcessWindowsFunction extends ProcessWindowFunction<ParseResult, Tuple2<Map<String, Object>, Iterable<ParseResult>>,Long, TimeWindow> {

        @Override
        public void process(Long key,Context context, Iterable<ParseResult> elements, Collector<Tuple2<Map<String, Object>, Iterable<ParseResult>>> out) {
            final Map<String, Object> statMetricsMap = Maps.newHashMap();
            Iterator<ParseResult> iterator = elements.iterator();
            Long successEventCount = 0L;
            Long failEventCount = 0L;
            RuntimeContext runtimeContext=getRuntimeContext();
            while (iterator.hasNext()) {
                ParseResult parseResult = iterator.next();
                if (parseResult.isParseSuccess()) {
                    successEventCount++;
                    Map<String, Object> keyValueMap = parseResult.getKeyValuesMap();
                    keyValueMap.forEach((k, v) -> {
                        if (v != null) {
                            Map<String, String> keyMetricsMap = (Map<String, String>) statMetricsMap.getOrDefault(k, Maps.<String, String>newHashMap());
                            Long originCnt = Long.parseLong(keyMetricsMap.getOrDefault("valueCount","0"));
                            keyMetricsMap.put("valueCount", String.valueOf(originCnt + 1L));
                            String valueType = v.getClass().getSimpleName();
                            keyMetricsMap.put("valueType", valueType);
                            if (valueType.equals("Double")|| valueType.equals("Float") || valueType.equals("Integer") || valueType.equals("Long")) {
                                if (!keyMetricsMap.containsKey("minValue") || Double.parseDouble(keyMetricsMap.get("minValue")) > Double.parseDouble(String.valueOf(v)))
                                    keyMetricsMap.put("minValue", String.valueOf(v));
                                if (!keyMetricsMap.containsKey("maxValue") || Double.parseDouble(keyMetricsMap.get("maxValue")) < Double.parseDouble(String.valueOf(v)))
                                    keyMetricsMap.put("maxValue", String.valueOf(v));
                            }
                            else{
                                if (!keyMetricsMap.containsKey("minValue") || keyMetricsMap.get("minValue") .compareTo(String.valueOf(v))>0)
                                    keyMetricsMap.put("minValue", String.valueOf(v));
                                if (!keyMetricsMap.containsKey("maxValue") || keyMetricsMap.get("maxValue") .compareTo(String.valueOf(v))<0)
                                    keyMetricsMap.put("maxValue", String.valueOf(v));
                            }
                            statMetricsMap.put(k, keyMetricsMap);
                        }
                    });
                } else
                    failEventCount++;
            }
            statMetricsMap.put("successEventCount", successEventCount);
            statMetricsMap.put("failEventCount", failEventCount);
            statMetricsMap.put("statPeriodBegin", LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getStart()), ZoneId.systemDefault()));
            statMetricsMap.put("statPeriodEnd", LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getEnd()), ZoneId.systemDefault()));
            Tuple2<Map<String, Object>, Iterable<ParseResult>> tuple2 = Tuple2.of(statMetricsMap, elements);
            out.collect(tuple2);
        }
    }


    public static class TestingKafkaRecordDeserializer
            implements KafkaRecordDeserializer<MetaAndValue> {
        private static final long serialVersionUID = -3765473065594331694L;
        private transient Deserializer<String> deserializer = new StringDeserializer();

        int parseNum=0;
        @Override
        public void deserialize(
                ConsumerRecord<byte[], byte[]> record, Collector<MetaAndValue> collector) {
            if (deserializer == null)
                deserializer = new StringDeserializer();
            MetaAndValue metaAndValue=new MetaAndValue(
                    new TopicPartition(record.topic(), record.partition()),
                    deserializer.deserialize(record.topic(), record.value()), record.offset());
            if(parseNum++>100) {
                Map<String,Object> metaData=metaAndValue.getMetaData();
                throw new RuntimeException("for test");
            }
            collector.collect(metaAndValue);
        }

        @Override
        public TypeInformation<MetaAndValue> getProducedType() {
            return TypeInformation.of(MetaAndValue.class);
        }
    }

    private static class DataRowStreamOperator extends AbstractStreamOperator<Row>
            implements OneInputStreamOperator<Tuple2<Map<String, Object>, Iterable<ParseResult>>, Row> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(StreamRecord<Tuple2<Map<String, Object>, Iterable<ParseResult>>> element) {
            RuntimeContext runtimeContext=getRuntimeContext();
            Iterable<ParseResult> iterable = element.getValue().f1;
            iterable.forEach(entry -> {
                if (entry.isParseSuccess()) {
                    ParseResult parseResult = entry;
                    Schema schema = ParseResult.getSchema();
                    Row row = new Row(schema.columns().size());
                    List<Types.NestedField> nestedFieldList = schema.columns();
                    Map<String, Object> keyValuesMap = parseResult.getKeyValuesMap();
                    for (int index = 0; index < row.getArity(); index++) {
                        Types.NestedField field = nestedFieldList.get(index);
                        row.setField(index, keyValuesMap.get(field.name()));
                    }
                    try {
                        output.collect(new StreamRecord(row, parseResult.getProcessingTime()));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    static class MergeStatProcessAllWindowFunction extends ProcessAllWindowFunction<Map<String, Object>, Row, GlobalWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(Context context, Iterable<Map<String, Object>> elements, Collector<Row> output) {
            RuntimeContext runtimeContext=getRuntimeContext();
            Stream<Map<String, Object>> mapStream = StreamSupport.stream(elements.spliterator(), false);
            Optional<Map<String, Object>> statMap = mapStream.reduce(new StatMapMergeOperator());
            if (statMap.isPresent()) {
                Schema schema = ParseResult.getSchema();
                Row row = new Row(dataTypeByName.size() + schema.columns().size());
                List<Types.NestedField> nestedFieldList = schema.columns();
                int index = 0;
                for (String key : dataTypeByName.keySet()) {
                    row.setField(index++, statMap.get().get(key));
                }
                for (Types.NestedField field : nestedFieldList) {
                    row.setField(index++, statMap.get().get(field.name()));
                }
                output.collect(row);
            }
        }
    }

    public static class StatMapMergeOperator implements BinaryOperator<Map<String, Object>> {

        @Override
        public Map<String, Object> apply(Map<String, Object> map1, Map<String, Object> map2) {
            map1.forEach((key, value) -> map2.merge(key, value, (v1, v2) -> {
                if (key.equals("successEventCount")) {
                    return (Long) v1 + (Long) v2;
                } else if (key.equals("failEventCount")) {
                    return (Long) v1 + (Long) v2;
                } else if (key.equals("statPeriodBegin")) {
                    return ((LocalDateTime) v1).isBefore((LocalDateTime) v2) ? v1 : v2;
                } else if (key.equals("statPeriodEnd")) {
                    return ((LocalDateTime) v1).isAfter((LocalDateTime) v2) ? v1 : v2;
                } else if (value instanceof Map) {
                    String valueType = ((Map<String, String>) v2).get("valueType");
                    Map<String, String> metricsMap1 = (Map<String, String>) v1;
                    Map<String, String> metricsMap2 = (Map<String, String>) v2;
                    metricsMap2.put("valueCount", String.valueOf(Long.parseLong(metricsMap1.get("valueCount")) + Long.parseLong(metricsMap2.get("valueCount"))));
                    if (valueType.equals("Double") || valueType.equals("Integer") || valueType.equals("Long")) {
                        metricsMap2.put("maxValue", String.valueOf(Double.parseDouble(metricsMap1.get("maxValue")) > Double.parseDouble(metricsMap2.get("maxValue")) ? metricsMap1.get("maxValue") : metricsMap2.get("maxValue")));
                        metricsMap2.put("minValue", String.valueOf(Double.parseDouble(metricsMap1.get("minValue")) < Double.parseDouble(metricsMap2.get("minValue")) ? metricsMap1.get("minValue") : metricsMap2.get("minValue")));
                    }
                    return v2;
                }
                return null;
            }));
            return map2;
        }
    }
}
