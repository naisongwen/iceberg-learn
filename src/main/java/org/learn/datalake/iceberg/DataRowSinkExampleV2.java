package org.learn.datalake.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.io.CloseableIterable;
import org.learn.datalake.common.BoundedTestSource;
import org.learn.datalake.common.ExampleBase;
import org.learn.datalake.common.SimpleDataUtil;

import java.io.File;
import java.util.List;
import java.util.Locale;
import java.util.Map;

//https://github.com/ververica/flink-cdc-connectors

//Reference Iceberg TestFlinkIcebergSinkV2
public class DataRowSinkExampleV2 extends ExampleBase {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(100L);
//        env.setParallelism(1);
        TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
                SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

        List<List<Row>> elementsPerCheckpoint = ImmutableList.of(
                // Checkpoint #1
                ImmutableList.of(
                        row("+I", 1, "aaa")
//                        row("+U", 1, "ccc")
                ),
                // Checkpoint #2
                //在不同事务中更新需要-U
                ImmutableList.of(
                        row("-U", 1, "aaa"),
                        row("+U", 1, "bbb")
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

        DataStream<Row> dataStream = env.addSource(new BoundedTestSource<Row>(
                        elementsPerCheckpoint),
                ROW_TYPE_INFO);
        String warehouse = parameterTool.get("warehouse", "warehouse");
        String tblName="test_sink_V2";
        File warehouseDir = new File(warehouse,tblName);
        if (warehouseDir.exists())
            FileUtils.cleanDirectory(warehouseDir);
        warehouseDir.mkdirs();
        FileFormat format = FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name(), "catalog-type", "hadoop", "warehouse", warehouseDir.getAbsolutePath());
        String HADOOP_CATALOG = "iceberg_hadoop_catalog";
        CatalogLoader catalogLoader =
                CatalogLoader.hadoop(HADOOP_CATALOG, new Configuration(), properties);
        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier tableIdentifier = TableIdentifier.of(tblName);
        Table table;
        if (catalog.tableExists(tableIdentifier))
            table = catalog.loadTable(tableIdentifier);
        else table = SimpleDataUtil.createTable(warehouseDir.getAbsolutePath(), SimpleDataUtil.SCHEMA, properties, false);

        TableLoader tableLoader = TableLoader.fromHadoopTable(warehouseDir.getAbsolutePath());

        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        // Shuffle by the equality key, so that different operations of the same key could be wrote in order when
        // executing tasks in parallel.
        dataStream = dataStream.keyBy(row -> row.getField(0));
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
                .tableLoader(tableLoader)
                .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
                .writeParallelism(1)
                .equalityFieldColumns(ImmutableList.of("id"))//Arrays.asList("id"))
                .build();
        env.execute();
        table.refresh();

        try (
                CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            String data = Iterables.toString(iterable);
            System.out.println(data);
        }
    }
}
