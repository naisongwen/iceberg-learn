package org.learn.iceberg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED;

public class TestQuery {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //1.12默认时间属性为 EventTime 无需设置
        env.setParallelism(1);

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        tableEnv.getConfig().getConfiguration().setString(TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key(), "true");

        tableEnv.getCurrentDatabase();
        env.enableCheckpointing(10000);

        File warehouse = new File("warehouse/test_cdc_sink");
        //iceberg
        tableEnv.executeSql("CREATE CATALOG defaultCatalog " +
                "WITH (" +
                " 'type'='iceberg', " +
                " 'catalog-type'='hadoop', " +
                String.format(" 'warehouse'='%s')", warehouse));
        //" 'warehouse'='hdfs://10.201.0.212:8020/khnib')");

        Table table=tableEnv.sqlQuery("select o_orderkey,o_clerk from " +
                        " `defaultCatalog`.`defaultkhnDB`.`t_kafka_mysqlcdc_sink_pk_v2_10` /*+ OPTIONS('streaming'='false')*/ ");
        DataStream<Row> appendStreamTableResult = tableEnv.toAppendStream(table, Row.class);
        appendStreamTableResult.print();
        env.execute("stream job demo");

//        Table table = new HadoopTables().load(new File("warehouse/test_cdc_sink/defaultkhnDB/t_kafka_mysqlcdc_sink_pk_v2_10").getAbsolutePath());
//        table.refresh();
//        Iterable<Snapshot> snapshotList=table.snapshots();
//        try (
//                CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
//            String data = Iterables.toString(iterable);
//            System.out.println(data);
//            for (Snapshot snapshot : table.snapshots()) {
//                long snapshotId = snapshot.snapshotId();
//                try (CloseableIterable<Record> reader = IcebergGenerics.read(table)
//                        .useSnapshot(snapshotId)
//                        .select("*")
//                        .build()) {
//                    reader.forEach(System.out::print);
//                    System.out.println();
//                }
//            }
    }
}