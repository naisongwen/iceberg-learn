package org.learn.datalake.debezium;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.junit.rules.TemporaryFolder;
import org.learn.datalake.common.DBConfig;
import org.learn.datalake.common.SimpleDataUtil;

import java.io.File;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import static org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo;

//https://github.com/ververica/flink-cdc-connectors

public class MySqlSourceExample {
    private static final String HIVE_CATALOG = "iceberg_hive_catalog";
    private static final String HADOOP_CATALOG = "iceberg_hadoop_catalog";

    public static void main(String[] args) throws Exception {
        TableSchema schema =
                TableSchema.builder()
                        .add(TableColumn.of("id", DataTypes.INT()))
                        .add(TableColumn.of("key", DataTypes.STRING()))
                        .add(TableColumn.of("value", DataTypes.STRING()))
                        .build();
        RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
        DataType internalDataType = DataTypeUtils.transform(
                schema.toRowDataType(),
                TypeTransformations.TO_INTERNAL_CLASS);
        TypeInformation<RowData> rowDataTypeInfo =
                (TypeInformation<RowData>)fromDataTypeToTypeInfo(internalDataType);
        DebeziumDeserializationSchema deserialer =
                new RowDataDebeziumDeserializeSchema(
                        rowType,
                        rowDataTypeInfo,
                        (rowData, rowKind) -> {
                        },
                        ZoneId.of("Asia/Shanghai"));

        //properties.put("snapshot.mode", "schema_only");
        /****
         * This can be controlled by the option debezium.snapshot.mode, you can set to:
         *
         * never: Specifies that the connect should never use snapshots and that upon first startup
         * with a logical server name the connector should read from the beginning of the binlog;
         * this should be used with care, as it is only valid when the binlog is guaranteed to
         * contain the entire history of the database.
         *
         * schema_only: If you don’t need a consistent snapshot of the data but only need them to
         * have the changes since the connector was started, you can use the schema_only option,
         * where the connector only snapshots the schemas (not the data).
         */
        SourceFunction<RowData> sourceFunction = MySQLSource.<RowData>builder()
                .hostname(DBConfig.MYSQLJKDB.host)
                .port(DBConfig.MYSQLJKDB.port)
                .databaseList(DBConfig.MYSQLJKDB.dbName)
                .username(DBConfig.MYSQLJKDB.userName)
                .password(DBConfig.MYSQLJKDB.passWd)
                .deserializer(deserialer)
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(5 * 1000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        checkpointConfig.setCheckpointTimeout(120 * 1000L);
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<RowData> streamSource = env.addSource(sourceFunction);

        TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder(new File("target"));
        TEMPORARY_FOLDER.create();
        File folder = TEMPORARY_FOLDER.newFolder();
        String warehouse = folder.getAbsolutePath();
        String tablePath = warehouse.concat("/test");

        FileFormat format=FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
        Table table = SimpleDataUtil.createTable(tablePath, props, false);


        TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);

        // need to upgrade version to 2,otherwise 'java.lang.IllegalArgumentException: Cannot write
        // delete files in a v1 table'
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        FlinkSink.forRowData(streamSource)
                .table(table)
                .tableLoader(tableLoader)
                .equalityFieldColumns(Arrays.asList("id"))
                .writeParallelism(1)
                .build();
        env.execute();
    }
}
