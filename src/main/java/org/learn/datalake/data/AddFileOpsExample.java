package org.learn.datalake.data;

import com.google.common.collect.Iterables;
import org.apache.iceberg.*;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.learn.datalake.metadata.TableTestBase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iceberg.Files.localInput;

//Reference TestRewriteDataFilesAction
public class AddFileOpsExample extends TableTestBase {
    public AddFileOpsExample() {
        super(2);
    }

    public static void main(String[] args) throws Exception {
        File warehouse=new File("warehouse/test_add_file");
        Table table = getTableOrCreate(warehouse,false);
        List<GenericRecord> recordList = mockInsertData();
        DataFile dataFile = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-1.parquet"));
        table.newAppend()
                .appendFile(dataFile)
                .commit();
        recordList = mockUpsertData();
        dataFile = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-2.parquet"));
        table.newAppend()
                .appendFile(dataFile)
                .commit();
        Actions actions = Actions.forTable(table);

        RewriteDataFilesActionResult result = actions
                .rewriteDataFiles()
                .splitOpenFileCost(1)
                .execute();
        CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
        List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));

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

    static List<GenericRecord> mockInsertData() {
        List<GenericRecord> records = new ArrayList<>();
        GenericRecord rec = GenericRecord.create(SCHEMA);
        rec.setField("id", 1);
        rec.setField("data", "aaa");
        records.add(rec);
        rec = GenericRecord.create(SCHEMA);
        rec.setField("id", 1);
        rec.setField("data", "bbb");
        records.add(rec);
        return records;
    }

    static List<GenericRecord> mockUpsertData() {
        List<GenericRecord> records = new ArrayList<>();
        GenericRecord rec = GenericRecord.create(SCHEMA);
        rec.setField("id", 1);
        rec.setField("data", "bbb");
        records.add(rec);
        return records;
    }
}
