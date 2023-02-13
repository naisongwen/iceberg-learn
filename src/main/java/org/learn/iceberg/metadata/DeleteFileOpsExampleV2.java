package org.learn.iceberg.metadata;

import com.google.common.collect.Iterables;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.StructLikeSet;
import org.learn.iceberg.common.TableTestBase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

//Reference DeleteReadTests
public class DeleteFileOpsExampleV2 extends TableTestBase {

    public static void main(String[] args) throws Exception {
        File warehouse = new File("warehouse/test_del_file_V2");
        Table table = getTableOrCreate(warehouse,true);
        List<GenericRecord> recordList = mockInsertData();
        DataFile dataFile = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), UUID.randomUUID()+".parquet"));
        table.newAppend()
                .appendFile(dataFile)
                .commit();
        recordList = mockDeleteData();
        File output=new File(new File(warehouse.getAbsolutePath() + "/data/"), UUID.randomUUID()+".parquet");
        DeleteFile deleteFile = equalityDelete(table,output, null, recordList);
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));
        table.newRowDelta()
                .addRows(dataFile)
                .addDeletes(deleteFile)
                .commit();

//        CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
//        List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));

        StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
        try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select("*").build()) {
            reader.forEach(set::add);
        }

        table.refresh();
        CloseableIterable<Record> iterable = IcebergGenerics.read(table).build();
        String data = Iterables.toString(iterable);
        System.out.println(data);
    }

    static List<GenericRecord> mockInsertData() {
        List<GenericRecord> records = new ArrayList<>();
        GenericRecord rec = GenericRecord.create(SCHEMA);
        rec.setField("id", 1);
        rec.setField("data", "aaa");
        records.add(rec);
        rec = GenericRecord.create(SCHEMA);
        rec.setField("id", 2);
        rec.setField("data", "bbb");
        records.add(rec);
        return records;
    }

    static List<GenericRecord> mockDeleteData() {
        List<GenericRecord> records = new ArrayList<>();
        GenericRecord rec = GenericRecord.create(SCHEMA);
        rec.setField("id", 1);
        rec.setField("data", "aaa");
        records.add(rec);
        return records;
    }
}
