package org.learn.datalake.metadata;

import com.google.common.collect.Lists;
import org.apache.iceberg.*;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.Assert;
import org.learn.datalake.common.TableTestBase;

import java.io.File;
import java.util.List;

//Reference:TestRewriteFiles
public class FileReWriteExampleV3 extends TableTestBase {

    public static void main(String[] args) throws Exception {

        File warehouse = new File("warehouse/test_rewrite_file_V3");
        Table table = getTableOrCreate(warehouse, true);
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        GenericRecord record = GenericRecord.create(table.schema());

        File outputA=new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-file-a.parquet");
        List<GenericRecord> recordList = Lists.newArrayList((GenericRecord)record.copy("id", 1, "data", "aaa"));
        DataFile dataFileA = writeParquetFile(table, recordList, outputA);

        File outputB=new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-file-b.parquet");
        recordList = Lists.newArrayList((GenericRecord)record.copy("id", 2, "data", "bbb"));
        DataFile dataFileB = writeParquetFile(table, recordList,outputB);

        table.newAppend()
                .appendFile(dataFileA)
                .commit();

        table.newAppend()
                .appendFile(dataFileB)
                .commit();

//        printTableData(table);
        Snapshot snapshotAfterDeletes = table.currentSnapshot();
        Assert.assertEquals("Should create 2 manifests for initial write", 2, snapshotAfterDeletes.allManifests().size());
        //printManifest(baseSnap);

        Actions actions = Actions.forTable(table);
        RewriteDataFilesActionResult result = actions
                .rewriteDataFiles()
                .splitOpenFileCost(1)
                .execute();

        CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
        List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
        Snapshot snapshot = table.currentSnapshot();
        printManifest(snapshot);
        printTableData(table);
    }
}
