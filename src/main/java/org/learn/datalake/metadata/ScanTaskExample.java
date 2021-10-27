package org.learn.datalake.metadata;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

public class ScanTaskExample extends TableTestBase {
    public ScanTaskExample() {
        super(1);
    }

    public static void main(String[] args) throws Exception {
        File tabDir = new File("warehouse/test_scan");
        if (tabDir.exists())
            FileUtils.cleanDirectory(tabDir);
        tabDir.mkdirs();
        ScanTaskExample manifestExample = new ScanTaskExample();
        manifestExample.setupTable(tabDir);
        manifestExample.testFileScan();
    }

    public void testFileScan() throws IOException {
        GenericRecord record1 = GenericRecord.create(schema);
        record1.setField("id", 1);
        record1.setField("name", "lily");
        record1.setField("age", 13);
        record1.setField("ts", LocalDateTime.parse("2003-01-01T00:01:00"));
        List<GenericRecord> records = Lists.newArrayList(record1);

        String location1 = hadoopTab.location().replace("file:", "") + "/data/file1.avro";

        try (FileAppender<GenericRecord> writer = Avro.write(Files.localOutput(location1))
                .schema(schema)
                .createWriterFunc(DataWriter::create)
                .build()) {
            writer.addAll(records);
        }

        DataFile file = DataFiles.builder(hadoopTab.spec())
                .withRecordCount(1L)
                .withPath(location1)
                .withFileSizeInBytes(Files.localInput(location1).getLength())
                .build();

        hadoopTab.newAppend().appendFile(file).commit();
        TableScan scan = hadoopTab.newScan();

        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
            for (FileScanTask task : tasks) {

            }
        }
    }
}
