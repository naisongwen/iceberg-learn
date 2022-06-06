package org.learn.datalake.transaction;

import com.google.common.collect.Iterables;
import org.apache.commons.collections.IteratorUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.junit.Assert;
import org.learn.datalake.common.TableTestBase;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

//Reference TestTransaction,TestIncrementalDataTableScan
public class TransactionExampleV2 extends TableTestBase {

    public static void main(String[] args) throws Exception {
        File warehouse=new File("warehouse/test_transaction_V2");
        Table table = getTableOrCreate(warehouse,true);

        Transaction transaction = table.newTransaction();

        add(transaction.newAppend(), files("A")); // 1
        add(transaction.newAppend(), files("B"));
        add(transaction.newAppend(), files("C"));
        add(transaction.newAppend(), files("D"));
        add(transaction.newAppend(), files("E")); // 5
        transaction.commitTransaction();
        //Streams.stream(table.snapshots().)
        List<Snapshot> list=IteratorUtils.toList(table.snapshots().iterator());
        //1~5
        filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(table,list.get(0).snapshotId(), list.get(list.size()-1).snapshotId()));

        transaction = table.newTransaction();
        replace(transaction.newRewrite(), files("A", "B", "C"), files("F", "G")); // 6
        transaction.commitTransaction();
        // Replace commits are ignored
        filesMatch(Lists.newArrayList("B", "C", "D", "E"), appendsBetweenScan(table,list.get(0).snapshotId(), list.get(list.size()-1).snapshotId()));
        transaction = table.newTransaction();
        delete(transaction.newDelete(), files("D")); // 7
        transaction.commitTransaction();
        // 7th snapshot is a delete.
        Assert.assertTrue("Replace and delete commits are ignored", appendsBetweenScan(table,list.get(list.size()-2).snapshotId(), list.get(list.size()-1).snapshotId()).isEmpty());
        Assert.assertTrue("Delete commits are ignored", appendsBetweenScan(table,list.get(list.size()-2).snapshotId(), list.get(list.size()-1).snapshotId()).isEmpty());
    }

    // Partition spec used to create tables
    protected static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
            .bucket("data", 16)
            .build();

    static DataFile file(String name) {
        return DataFiles.builder(SPEC)
                .withPath(name + ".parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build();
    }

    static List<DataFile> files(String... names) {
        return Lists.transform(Lists.newArrayList(names), TransactionExampleV2::file);
    }

    private static void add(AppendFiles appendFiles, List<DataFile> adds) {
        for (DataFile f : adds) {
            appendFiles.appendFile(f);
        }
        appendFiles.commit();
    }

    private static void delete(DeleteFiles deleteFiles, List<DataFile> deletes) {
        for (DataFile f : deletes) {
            deleteFiles.deleteFile(f);
        }
        deleteFiles.commit();
    }

    private static void replace(RewriteFiles rewriteFiles, List<DataFile> deletes, List<DataFile> adds) {
        rewriteFiles.rewriteFiles(Sets.newHashSet(deletes), Sets.newHashSet(adds));
        rewriteFiles.commit();
    }

    private static void overwrite(OverwriteFiles overwriteFiles, List<DataFile> adds, List<DataFile> deletes) {
        for (DataFile f : adds) {
            overwriteFiles.addFile(f);
        }
        for (DataFile f : deletes) {
            overwriteFiles.deleteFile(f);
        }
        overwriteFiles.commit();
    }

    private static List<String> appendsBetweenScan(Table table, long fromSnapshotId, long toSnapshotId) {
        Snapshot s1 = table.snapshot(fromSnapshotId);
        Snapshot s2 = table.snapshot(toSnapshotId);
        TableScan appendsBetween = table.newScan().appendsBetween(s1.snapshotId(), s2.snapshotId());
        return filesToScan(appendsBetween);
    }

    private static List<String> filesToScan(TableScan tableScan) {
        Iterable<String> filesToRead = org.apache.iceberg.relocated.com.google.common.collect.Iterables.transform(tableScan.planFiles(), t -> {
            String path = t.file().path().toString();
            return path.split("\\.")[0];
        });
        return Lists.newArrayList(filesToRead);
    }

    private static void filesMatch(List<String> expected, List<String> actual) {
        Collections.sort(expected);
        Collections.sort(actual);
        Assert.assertEquals(expected, actual);
    }

}
