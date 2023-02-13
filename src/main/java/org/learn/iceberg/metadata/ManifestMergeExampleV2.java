package org.learn.iceberg.metadata;

import com.google.common.collect.Iterables;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.learn.iceberg.common.TableTestBase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

//Reference TestMergeAppend,TestRewriteManifests,TestManifestCleanup
public class ManifestMergeExampleV2 extends TableTestBase {

    public static void main(String[] args) throws Exception {
        File warehouse=new File("warehouse/test_manifest_V2");
        Table table = getTableOrCreate(warehouse,true);
        List<GenericRecord> recordList = mockInsertData();
        DataFile dataFileA = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-1.parquet"));
        DataFile dataFileB = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-2.parquet"));


        table.updateProperties()
                .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1")
                .commit();

        table.newAppend()
                .appendFile(dataFileA)
                .commit();

        table.newAppend()
                .appendFile(dataFileB)
                .commit();

        Snapshot snapshot = table.currentSnapshot();
        printManifest(snapshot, table.io());

//        File manifestFile=new File(new File(warehouse.getAbsolutePath() + "/metadata/"), "manifest-0.avro");
//        ManifestFile manifest = writeManifest(null,table,manifestFile,dataFileA, dataFileB);
//        table.newAppend()
//                .appendManifest(manifest)
//                .commit();

        CloseableIterable<Record> iterable = IcebergGenerics.read(table).build();
        String data = Iterables.toString(iterable);
        System.out.println(data);
        snapshot = table.currentSnapshot();
        printManifest(snapshot, table.io());
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
