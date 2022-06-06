package org.learn.datalake.metadata;

import com.google.common.collect.Iterables;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.learn.datalake.common.Constants;
import org.learn.datalake.common.SimpleDataUtil;
import org.learn.datalake.common.TableTestBase;

import java.io.File;
import java.util.List;
import java.util.UUID;

//Reference DeleteReadTests
public class AddFileOpsExample extends TableTestBase {

    public static void main(String[] args) throws Exception {
        File warehouse = new File("warehouse/default/test_add_file");
        Table table = getOrCreateHiveTable(warehouse.getAbsolutePath(), SimpleDataUtil.SCHEMA, Constants.hmsUri, true);

        List<GenericRecord> mockInsertRecordList = mockInsertRecords();
        File addFile=new File(new File(warehouse.getAbsolutePath() + "/data/"), UUID.randomUUID() + ".parquet");
        DataFile dataFile = writeParquetFile(table, mockInsertRecordList,addFile );
        table.newAppend()
                .appendFile(dataFile)
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
}
