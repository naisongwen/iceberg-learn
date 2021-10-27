import com.google.common.collect.Iterables;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import java.io.File;
import java.util.ArrayList;

public class TestQuery {
    public static void main(String[] args) throws Exception {
        Table table = new HadoopTables().load(new File("warehouse/test_upsert_file_V4").getAbsolutePath());
        table.refresh();
        Iterable<Snapshot> snapshotList=table.snapshots();
        try (
                CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            String data = Iterables.toString(iterable);
            System.out.println(data);
            for (Snapshot snapshot : table.snapshots()) {
                long snapshotId = snapshot.snapshotId();
                try (CloseableIterable<Record> reader = IcebergGenerics.read(table)
                        .useSnapshot(snapshotId)
                        .select("*")
                        .build()) {
                    reader.forEach(System.out::print);
                    System.out.println();
                }
            }
        }
    }
}
