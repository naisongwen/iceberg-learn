package org.learn.datalake.data;

import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.learn.datalake.metadata.TableTestBase;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.iceberg.Files.localInput;

//Reference DeleteReadTests
public class DeleteFileOpsExample extends TableTestBase {
    public DeleteFileOpsExample() {
        super(2);
    }

    public static void main(String[] args) throws Exception {
        File warehouse = new File("warehouse/test_del_file");
        Table table = getTableOrCreate(warehouse,false);
        List<GenericRecord> recordList = mockInsertData();
        DataFile dataFile = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), UUID.randomUUID()+".parquet"));
        table.newAppend()
                .appendFile(dataFile)
                .commit();
        recordList = mockDeleteData();
        File output=new File(new File(warehouse.getAbsolutePath() + "/data/"), UUID.randomUUID()+".parquet");
        DeleteFile eqDeletes = writeDeleteFile(table,Files.localOutput(output), null, recordList, SCHEMA);
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));
        table.newRowDelta()
                .addDeletes(eqDeletes)
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

    static DataFile writeParquetFile(Table table, List<GenericRecord> records, File parquetFile) throws IOException {
        FileAppender<GenericRecord> appender = Parquet.write(Files.localOutput(parquetFile))
                .schema(table.schema())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .build();
        try {
            appender.addAll(records);
        } finally {
            appender.close();
        }

        PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
        return DataFiles.builder(table.spec())
                .withPartition(partitionKey)
                .withInputFile(localInput(parquetFile))
                .withMetrics(appender.metrics())
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    static DeleteFile writeEqDeletes(PartitionSpec spec,File output) {
        return FileMetadata.deleteFileBuilder(spec)
                .ofEqualityDeletes(0)
                .withPath(output.getAbsolutePath())
                .build();
    }

    public static DeleteFile writeDeleteFile(Table table, OutputFile out, StructLike partition,
                                             List<GenericRecord> deletes, Schema deleteRowSchema) throws IOException {
        EqualityDeleteWriter<GenericRecord> writer = Parquet.writeDeletes(out)
                .forTable(table)
                .withPartition(partition)
                .rowSchema(deleteRowSchema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .equalityFieldIds(deleteRowSchema.columns().stream().mapToInt(Types.NestedField::fieldId).toArray())
                .buildEqualityWriter();

        try (Closeable toClose = writer) {
            writer.deleteAll(deletes);
        }

        return writer.toDeleteFile();
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
