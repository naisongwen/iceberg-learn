import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.io.CloseableIterable;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TestFileScan {

    public static void main(String[] args) {
//        File dataWarehouseDir = new File("warehouse", "kafka_iceberg_data");
//        TableLoader dataTableLoader = TableLoader.fromHadoopTable(dataWarehouseDir.getAbsolutePath());
//        dataTableLoader.open();
//        Table dataTable=dataTableLoader.loadTable();
//        Actions actions = Actions.forTable(dataTable);
//        RewriteDataFilesActionResult result = actions
//                .rewriteDataFiles()
//                .splitOpenFileCost(1)
//                .execute();
//        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
//        DateTime startDateTime= fmt.parseDateTime("2021-12-10 19:00:00");
//        DateTime endDateTime= fmt.parseDateTime("2021-12-10 20:00:00");
//        CloseableIterable<Record> iterable = IcebergGenerics.read(dataTable).where(
//                Expressions.and(Expressions.greaterThanOrEqual("__processing_time__",startDateTime.getMillis()),
//                Expressions.lessThan("__processing_time",endDateTime.getMillis())))
//                .build();
//        StreamSupport.stream(iterable.spliterator(),true);
//        iterable.forEach(record -> {
//            System.out.println(record.toString());
//        });

        File warehouse = new File("warehouse/default/test_flink_sink2");
        TableLoader statTableLoader = TableLoader.fromHadoopTable(warehouse.getAbsolutePath());
        statTableLoader.open();
        Table statTable = statTableLoader.loadTable();
        CloseableIterable<Record> iterable = IcebergGenerics.read(statTable)
                .where(Expressions.equal("col_value","xxxxx"))
                .build();
        iterable.forEach(record -> {
            System.out.println(record.toString());
        });
    }
}
