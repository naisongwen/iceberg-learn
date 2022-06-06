import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import org.learn.datalake.common.Constants;
import org.learn.datalake.common.MetaAndValue;
import org.learn.datalake.iceberg.KafkaSinkIcebergExample;

import java.io.File;

public class KafkaConsumerTest {
    @Test
    public void testKafka() throws Exception {
        final String topic = "sample_topic";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new FsStateBackend("file://" + new File("checkpoint-dir").getAbsolutePath()));
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.seconds(0)));
        env.getCheckpointConfig().setCheckpointInterval(1 * 100L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        KafkaRecordDeserializer recordDeserializer = new KafkaSinkIcebergExample.TestingKafkaRecordDeserializer();
        KafkaSource<MetaAndValue> kafkaSource =
                KafkaSource.<ObjectNode>builder()
                        .setBootstrapServers(Constants.kafkaServers)
                        .setGroupId(KafkaSinkIcebergExample.class.getName())
                        .setTopics(topic)
                        .setDeserializer(recordDeserializer)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setBounded(OffsetsInitializer.latest())
                        .build();

        //多分区，如果某分区拉取完成会导致CK无法完成
        DataStream<MetaAndValue> stream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(), "Read Kafka").setParallelism(1);
        stream.print();
        env.execute();
    }
}
