package flink.source;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.SteppingMailboxProcessor;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.learn.datalake.common.CatalogUtil;
import org.learn.datalake.common.TableTestBase;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class StreamOperatorReaderTest extends TableTestBase {
    public final String catalog = "iceberg_default_42";
    public final String database = "default";
    public final String table = "wns_test_tbl_3";
    public final String statTable = "order_kafka2_stat";

    public final String warehouse = "hdfs://10.201.0.82:9000/dlink_test/catalogmanager/test/";
    String thriftUri="thrift://10.201.0.203:9083";


    private static final FileFormat DEFAULT_FORMAT = FileFormat.PARQUET;
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private List<FlinkInputSplit> generateSplits(Table table) {
        List<FlinkInputSplit> inputSplits = Lists.newArrayList();

        List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
        for (int i = snapshotIds.size() - 1; i >= 0; i--) {
            ScanContext scanContext;
            if (i == snapshotIds.size() - 1) {
                // Generate the splits from the first snapshot.
                scanContext = ScanContext.builder()
                        .useSnapshotId(snapshotIds.get(i))
                        .build();
            } else {
                // Generate the splits between the previous snapshot and current snapshot.
                scanContext = ScanContext.builder()
                        .startSnapshotId(snapshotIds.get(i + 1))
                        .endSnapshotId(snapshotIds.get(i))
                        .build();
            }
            Collections.addAll(inputSplits,FlinkSplitGenerator.createInputSplits(table, scanContext));
        }
        return inputSplits;
    }

    private SteppingMailboxProcessor createLocalMailbox(
            OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness) {
        return new SteppingMailboxProcessor(
                MailboxDefaultAction.Controller::suspendDefaultAction,
                harness.getTaskMailbox(),
                StreamTaskActionExecutor.IMMEDIATE);
    }

    private OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> createReader(TableLoader tableLoader) throws Exception {
        // This input format is used to opening the emitted split.
        FlinkInputFormat inputFormat = FlinkSource.forRowData()
                .tableLoader(tableLoader)
                .buildFormat();

        OneInputStreamOperatorFactory<FlinkInputSplit, RowData> factory = StreamingReaderOperator.factory(inputFormat);
        OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness = new OneInputStreamOperatorTestHarness<>(
                factory, 1, 1, 0);
        harness.getStreamConfig().setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        return harness;
    }

    private List<List<Record>> generateRecordsAndCommitTxn(Table table,int commitTimes) throws IOException {
        List<List<Record>> expectedRecords = Lists.newArrayList();
        for (int i = 0; i < commitTimes; i++) {
            List<Record> records = RandomGenericData.generate(SCHEMA, 100, 0L);
            expectedRecords.add(records);

            // Commit those records to iceberg table.
            writeRecords(table,records);
        }
        return expectedRecords;
    }

    private void writeRecords(Table table,List<Record> records) throws IOException {
        GenericAppenderHelper appender = new GenericAppenderHelper(table, DEFAULT_FORMAT, temp);
        appender.appendToTable(records);
    }

    @Test
    public void main() throws Exception {

//        File  warehouse=new File("warehouse/default/operator_test");
//        Table table=getTableOrCreate(warehouse,true);
//        TableLoader tableLoader = TableLoader.fromHadoopTable(warehouse.getAbsolutePath());
//        List<List<Record>> expectedRecords = generateRecordsAndCommitTxn(table,15);


        TableIdentifier statIdentifier = TableIdentifier.of(database, statTable);
        CatalogLoader catalogLoader = CatalogUtil.getHiveCatalogLoader(catalog, warehouse, thriftUri);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, statIdentifier);
        tableLoader.open();
        Table table = tableLoader.loadTable();

        List<FlinkInputSplit> splits = generateSplits(table);
        Assert.assertTrue("Should have 1+ splits",splits.size()>0);

        try (OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness = createReader(tableLoader)) {
            harness.setup();
            harness.open();

            // Enqueue all the splits.
            for (FlinkInputSplit split : splits) {
                harness.processElement(split, -1);
            }

            // Read all records from the first five splits.
            SteppingMailboxProcessor localMailbox = createLocalMailbox(harness);
            for (int i = 0; i < 5; i++) {
                Assert.assertTrue("Should have processed the split#" + i, localMailbox.runMailboxStep());
            }
        }
    }
}
