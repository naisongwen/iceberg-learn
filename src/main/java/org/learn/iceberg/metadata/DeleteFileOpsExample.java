package org.learn.iceberg.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.learn.iceberg.common.TableTestBase;

import java.io.File;
import java.util.List;
import java.util.UUID;

//Reference DeleteReadTests
public class DeleteFileOpsExample extends TableTestBase {

    public static void main(String[] args) throws Exception {
        File warehouse = new File("warehouse/test_del_file");
        Table table = getTableOrCreate(warehouse, true);

        List<GenericRecord> mockInsertRecordList = mockInsertRecords();
        File addFile=new File(new File(warehouse.getAbsolutePath() + "/data/"), UUID.randomUUID() + ".parquet");
        DataFile dataFile = writeParquetFile(table, mockInsertRecordList,addFile );
        table.newAppend()
                .appendFile(dataFile)
                .commit();

        List<GenericRecord> mockDeleteRecords = mockDeleteRecords();
        File outFile=new File(new File(warehouse.getAbsolutePath() + "/data/"), UUID.randomUUID() + ".parquet");
        DeleteFile deleteFile = posDelete(table, mockDeleteRecords,addFile, outFile);
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));
        table.newRowDelta()
                .addRows(dataFile)
                .addDeletes(deleteFile)
                .commit();

        CloseableIterable<Record> iterable = IcebergGenerics.read(table).build();
        String data = Iterables.toString(iterable);
        System.out.println(data);

    }

    static ImmutableList mockInsertRecords() {
        GenericRecord record = GenericRecord.create(SCHEMA);

        ImmutableList.Builder<Record> builder = ImmutableList.builder();
        builder.add(record.copy(ImmutableMap.of("id", 1, "data", "a")));
        builder.add(record.copy(ImmutableMap.of("id", 2, "data", "bb")));
        builder.add(record.copy(ImmutableMap.of("id", 3, "data", "ccc")));
        builder.add(record.copy(ImmutableMap.of("id", 4, "data", "dddd")));

        ImmutableList records = builder.build();
        return records;
    }


    static ImmutableList mockDeleteRecords() {
        GenericRecord record = GenericRecord.create(SCHEMA);

        ImmutableList.Builder<Record> builder = ImmutableList.builder();
        builder.add(record.copy(ImmutableMap.of("id", 1, "data", "a")));
        builder.add(record.copy(ImmutableMap.of("id", 2, "data", "b")));

        ImmutableList records = builder.build();
        return records;
    }
}
