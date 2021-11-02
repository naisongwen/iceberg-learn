package org.learn.datalake.metadata;

import com.google.common.collect.Iterables;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.learn.datalake.common.TableTestBase;
import org.mockito.internal.util.collections.Sets;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.iceberg.Files.localInput;

//Reference:TestRewriteFiles
public class FileReWriteExample extends TableTestBase {

    public static void main(String[] args) throws Exception {
        File warehouse = new File("warehouse/test_rewrite_file");
        Table table = getTableOrCreate(warehouse, true);
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));
        GenericRecord record = GenericRecord.create(table.schema());
        List<GenericRecord> recordList = Lists.newArrayList((GenericRecord)record.copy("id", 1, "data", "aaa"));
        DataFile dataFileA = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-file-a.parquet"));
        recordList = Lists.newArrayList((GenericRecord)record.copy("id", 2, "data", "bbb"));
        DataFile dataFileB = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-file-b.parquet"));
//        table.newRewrite()
//                .rewriteFiles(Sets.newSet(dataFileA),Sets.newSet(dataFileB))
//                .commit();
        File output=new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-file-c.parquet");
        recordList = Lists.newArrayList((GenericRecord)record.copy("id", 2, "data", "BBB"));
        DeleteFile deleteFileC = writeDeleteFile(table,Files.localOutput(output), null, recordList, table.schema());

        output=new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-file-d.parquet");
        recordList = Lists.newArrayList((GenericRecord)record.copy("id", 2, "data", "bbb"));
        DeleteFile deleteFileD = writeDeleteFile(table,Files.localOutput(output), null, recordList, table.schema());

        table.newRowDelta()
                .addRows(dataFileA)
                .addDeletes(deleteFileC)
                .commit();

        printTableData(table);
        Snapshot baseSnap = table.currentSnapshot();
        Assert.assertEquals("Should create 2 manifests for initial write", 2, baseSnap.allManifests().size());
        //printManifest(baseSnap);
        table.newRewrite()
                .rewriteFiles(Sets.newSet(dataFileA),Sets.newSet(deleteFileC),Sets.newSet(dataFileB),Sets.newSet(deleteFileD))
                .commit();
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
