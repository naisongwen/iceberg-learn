package org.learn.datalake.transaction;

import com.google.common.collect.Iterables;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.junit.Assert;
import org.learn.datalake.common.TableTestBase;
import org.learn.datalake.metadata.TestTables;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

//Reference TestTransaction,TestIncrementalDataTableScan
public class TransactionExample extends TableTestBase {
    public static void main(String[] args) throws Exception {
        File warehouse=new File("warehouse/test_transaction");
        Table table = getTableOrCreate(warehouse,true);
        List<GenericRecord> recordList = mockInsertData();
        DataFile dataFileA = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-1.parquet"));
        DataFile dataFileB = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-2.parquet"));

        // use only one retry
        table.updateProperties()
                .set(TableProperties.COMMIT_NUM_RETRIES, "1")
                .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
                .commit();

        //Assert.assertEquals("Table should be on version 1 after txn create", 1, );

        Transaction txn=table.newTransaction();
        txn.newAppend()
                .appendFile(dataFileA)
                .commit();

        // cause the transaction commit to fail
        txn.newAppend()
                .appendFile(dataFileB)
                .commit();
        txn.commitTransaction();
        Snapshot committedSnapshot = table.currentSnapshot();

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
        return records;
    }
}
