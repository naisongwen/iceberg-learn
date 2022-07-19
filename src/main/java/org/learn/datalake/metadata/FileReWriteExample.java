package org.learn.datalake.metadata;

import com.google.common.collect.Lists;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.learn.datalake.common.TableTestBase;
import org.mockito.internal.util.collections.Sets;

import java.io.File;
import java.util.List;

//Reference:TestRewriteFiles
public class FileReWriteExample extends TableTestBase {

    public static void main(String[] args) throws Exception {

        File warehouse = new File("warehouse/test_rewrite_file");
        Table table = getTableOrCreate(warehouse, true);
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        GenericRecord record = GenericRecord.create(table.schema());

        File outputA=new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-file-a.parquet");
        List<GenericRecord> recordList = Lists.newArrayList((GenericRecord)record.copy("id", 1, "data", "aaa"),(GenericRecord)record.copy("id", 2, "data", "bbb"));
        DataFile dataFileA = writeParquetFile(table, recordList, outputA);

        File outputB=new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-file-b.parquet");
        recordList = Lists.newArrayList((GenericRecord)record.copy("id", 2, "data", "bbb"),(GenericRecord)record.copy("id", 3, "data", "ccc"));
        DataFile dataFileB = writeParquetFile(table, recordList,outputB);

        File outputC=new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-file-c.parquet");
        recordList = Lists.newArrayList((GenericRecord)record.copy("id", 1, "data", "aaa"));
        DeleteFile deleteFileC = posDelete(table, recordList, outputA, outputC);

        File outputD=new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-file-d.parquet");
        recordList = Lists.newArrayList((GenericRecord)record.copy("id", 2, "data", "bbb"));
        DeleteFile deleteFileD = posDelete(table,recordList, outputB, outputD);

        table.newRowDelta()
                .addRows(dataFileA)
                .addDeletes(deleteFileC)
                .commit();

//        printTableData(table);
        Snapshot snapshotAfterDeletes = table.currentSnapshot();
//        Assert.assertEquals("Should create 2 manifests for initial write", 2, baseSnap.allManifests().size());
        //printManifest(baseSnap);
//        table.newRewrite().validateFromSnapshot(snapshotAfterDeletes.snapshotId())
//                .rewriteFiles(Sets.newSet(dataFileA),Sets.newSet(dataFileB))
//                .commit();
        table.newRewrite().validateFromSnapshot(snapshotAfterDeletes.snapshotId())
                .rewriteFiles(Sets.newSet(dataFileA),Sets.newSet(deleteFileC),Sets.newSet(dataFileB),Sets.newSet(deleteFileD))
                .commit();
//        table.newRewrite().validateFromSnapshot(snapshotAfterDeletes.snapshotId())
//                .rewriteFiles(Sets.newSet(dataFileB),Sets.newSet(deleteFileC),Sets.newSet(),Sets.newSet())
//                .commit();
//        Actions actions = Actions.forTable(table);
//        RewriteDataFilesActionResult result = actions
//                .rewriteDataFiles()
//                .splitOpenFileCost(1)
//                .execute();
        CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
        List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
        Snapshot snapshot = table.currentSnapshot();
        printManifest(snapshot);
        printTableData(table);
    }
}
