package org.learn.datalake.metadata;

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.io.FileUtils;
import org.apache.flink.calcite.shaded.com.google.common.collect.Streams;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

//Reference:TestFlinkAvroReaderWriter
public class MetadataExamples extends TableTestBase {
    public MetadataExamples() {
        super(2);
    }

    public static void main(String[] args) throws Exception {
        File tabDir = new File("warehouse/test_tab");
        if (tabDir.exists())
            FileUtils.cleanDirectory(tabDir);
        tabDir.mkdirs();
        MetadataExamples fileOperateExamples = new MetadataExamples();
        fileOperateExamples.setupTable(tabDir);
        fileOperateExamples.testFileReadWrite();
    }

    public void testFileReadWrite() throws IOException {
        GenericRecord record1 = GenericRecord.create(schema);
        record1.setField("id", 1);
        record1.setField("name", "lily");
        record1.setField("age", 13);
        record1.setField("ts", LocalDateTime.parse("2003-01-01T00:01:00"));
        List<GenericRecord> records = Lists.newArrayList(record1);

        String location1 = hadoopTab.location().replace("file:", "")+ "/data/file1.avro";
        // FileAppender writer = Avro.write(Files.localOutput(location1)).schema(schema).named("test2").build();
//        try(FileAppender writer = ORC.write(Files.localOutput(location1)).
//                schema(schema).
//                createWriterFunc(GenericOrcWriter::buildWriter).
//                build()){
//            writer.addAll(records);
//        }
        RowType flinkSchema = FlinkSchemaUtil.convert(schema);
        try (FileAppender<GenericRecord> writer = Avro.write(Files.localOutput(location1))
                .schema(schema)
                .createWriterFunc(DataWriter::create)
//                .createWriterFunc(ignore -> new FlinkAvroWriter(flinkSchema))
                .build()) {
            writer.addAll(records);
        }

        DataFile file = DataFiles.builder(hadoopTab.spec())
                .withRecordCount(1L)
                .withPath(location1)
                .withFileSizeInBytes(Files.localInput(location1).getLength())
                .build();

        hadoopTab.newAppend().appendFile(file).commit();

        hadoopTab.refresh();

        String manifestListLocation = hadoopTab.currentSnapshot().manifestListLocation().replace("file:", "");

        try (CloseableIterable<org.apache.iceberg.data.Record> reader = Avro.read(Files.localInput(location1))
                .project(schema)
                .createReaderFunc(DataReader::create)
                .build()) {

        }

        try(CloseableIterable<Record> reader = Avro.read(Files.localInput(location1))
                .project(schema)
                .createReaderFunc(DataReader::create)
                .build()) {
            Iterator<Record> iterator = reader.iterator();
            System.out.println(iterator.next());
        }
        CloseableIterable<org.apache.iceberg.data.Record> reader=IcebergGenerics.read(hadoopTab).build();
        List<org.apache.iceberg.data.Record> recordList = Lists.newArrayList(reader);

        Snapshot currentSnapshot = hadoopTab.currentSnapshot();
        List<ManifestFile> manifestFiles=currentSnapshot.allManifests();
        try (ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifestFiles.get(0), FILE_IO)
                .filterRows(Expressions.equal("id", 0))) {
            List<String> files = Streams.stream(manifestReader)
                    .map(f -> f.path().toString())
                    .collect(Collectors.toList());

        }
    }
}
