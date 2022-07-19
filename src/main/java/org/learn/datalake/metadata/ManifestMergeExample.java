package org.learn.datalake.metadata;

import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.learn.datalake.common.TableTestBase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED;

//Reference TestMergeAppend
public class ManifestMergeExample extends TableTestBase {
    public static void main(String[] args) throws Exception {
        File warehouse=new File("warehouse/test_manifest");
        Table table = getTableOrCreate(warehouse,true);
        List<GenericRecord> recordList = mockInsertData();
        DataFile dataFileA = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-1.parquet"));
        DataFile dataFileB = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-2.parquet"));
//        File manifestFile=new File(new File(warehouse.getAbsolutePath() + "/metadata/"), "manifest-0.avro");
//        ManifestFile manifest = writeManifest(null,table,manifestFile,dataFileA, dataFileB);
//
//        table.newAppend()
//                .appendFile(dataFileA)
//                .appendFile(dataFileB)
//                .appendManifest(manifest)
//                .commit();

        table.newAppend()
                .appendFile(dataFileA)
                .commit();

        table.newAppend()
                .appendFile(dataFileB)
                .commit();

        table.updateProperties().set(SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();
        table.rewriteManifests()
                .clusterBy(file -> "")
                .commit();

        Snapshot committedSnapshot = table.currentSnapshot();
        long snapshotId = committedSnapshot.snapshotId();
        List<ManifestFile> manifestFiles=committedSnapshot.allManifests();
        for(ManifestFile m:manifestFiles) {
            System.out.println(m.path()+" owns datafiles as below:");
            ManifestReader<DataFile> reader = ManifestFiles.read(m, new TestTables.LocalFileIO());
            List<String> files = Streams.stream(reader)
                    .map(file -> file.path().toString())
                    .collect(Collectors.toList());
            for (CloseableIterator<DataFile> it = reader.iterator(); it.hasNext(); ) {
                DataFile entry = it.next();
                System.out.println(entry.path());
            }
        }
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
