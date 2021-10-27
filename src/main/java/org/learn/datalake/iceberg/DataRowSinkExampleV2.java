package org.learn.datalake.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.rules.TemporaryFolder;
import org.learn.datalake.common.BoundedTestSource;
import org.learn.datalake.common.ExampleBase;
import org.learn.datalake.common.SimpleDataUtil;

import java.io.File;
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
        TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
                SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

        DataStream<Row> dataStream = env.addSource(new BoundedTestSource<>(
                        row("+I", 1, "aaa"),
                        row("+U", 1, "bbb"),
                        row("+I", 3, "ccc")),
                ROW_TYPE_INFO);

        TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder(new File("target"));
        TEMPORARY_FOLDER.create();
        File folder = TEMPORARY_FOLDER.newFolder();
        String warehouse = parameterTool.get("warehouse", "warehouse");
        File warehouseDir = new File(warehouse);
//        if (warehouse.exists())
//            FileUtils.cleanDirectory(warehouse);
//        warehouse.mkdirs();
        String tablePath = warehouseDir.getAbsolutePath().concat("/test");
        FileFormat format = FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name(),"catalog-type", "hadoop","warehouse",warehouseDir.getAbsolutePath());
        String HADOOP_CATALOG = "iceberg_hadoop_catalog";
        CatalogLoader catalogLoader =
                CatalogLoader.hadoop(HADOOP_CATALOG, new Configuration(), properties);
        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier tableIdentifier = TableIdentifier.of("test");
        Table table;
        if (catalog.tableExists(tableIdentifier))
            table = catalog.loadTable(tableIdentifier);
        else table = SimpleDataUtil.createTable(tablePath, properties, false);

        TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);

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
                .equalityFieldColumns(ImmutableList.of("id", "data"))//Arrays.asList("id"))
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
