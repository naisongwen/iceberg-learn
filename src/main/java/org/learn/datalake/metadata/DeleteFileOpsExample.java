package org.learn.datalake.metadata;

import com.google.common.collect.Iterables;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.learn.datalake.common.TableTestBase;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

//Reference DeleteReadTests
public class DeleteFileOpsExample extends TableTestBase {

    static ImmutableList createDeleteRecords() {
        GenericRecord record = GenericRecord.create(SCHEMA);

        ImmutableList.Builder<Record> builder = ImmutableList.builder();
        builder.add(record.copy(ImmutableMap.of("id", 1, "data", "a")));
        builder.add(record.copy(ImmutableMap.of("id", 2, "data", "b")));
        builder.add(record.copy(ImmutableMap.of("id", 3, "data", "c")));
        builder.add(record.copy(ImmutableMap.of("id", 4, "data", "d")));
        builder.add(record.copy(ImmutableMap.of("id", 5, "data", "e")));

        ImmutableList records = builder.build();
        return records;
    }

    static void writePosDelete(Table table,List<Record> recordList,File file) throws IOException {
        OutputFile out = Files.localOutput(new File(table.location() + "/data/", UUID.randomUUID() + ".avro"));
        PositionDeleteWriter<Record> deleteWriter = Parquet.writeDeletes(out)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .rowSchema(SCHEMA)
                .withSpec(PartitionSpec.unpartitioned())
                .buildPositionWriter();

        String deletePath = "s3://bucket/path/file.parquet";
        try (PositionDeleteWriter<Record> writer = deleteWriter) {
            for (int i = 0; i < recordList.size(); i += 1) {
                int pos = i * 3 + 2;
                writer.delete(deletePath, pos, recordList.get(i));
            }
        }

        DeleteFile metadata = deleteWriter.toDeleteFile();
        Assert.assertEquals("Format should be Parquet", FileFormat.PARQUET, metadata.format());
        Assert.assertEquals("Should be position deletes", FileContent.POSITION_DELETES, metadata.content());
        Assert.assertEquals("Record count should be correct", recordList.size(), metadata.recordCount());
        Assert.assertEquals("Partition should be empty", 0, metadata.partition().size());
        Assert.assertNull("Key metadata should be null", metadata.keyMetadata());

        List<Record> deletedRecords;
        try (CloseableIterable<Record> reader = Parquet.read(out.toInputFile())
                .project(table.schema())
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                .build()) {
            deletedRecords = Lists.newArrayList(reader);

        }
    }

    public static void main(String[] args) throws Exception {
        File warehouse = new File("warehouse/test_del_file");
        Table table = getTableOrCreate(warehouse, true);
        ImmutableList records = createDeleteRecords();
        OutputFile out = Files.localOutput(new File(warehouse.getAbsolutePath() + "/data/", UUID.randomUUID() + ".avro"));

        EqualityDeleteWriter<Record> deleteWriter =
                //Parquet.writeDeletes(out).createWriterFunc(GenericParquetWriter::buildWriter)
                Avro.writeDeletes(out).createWriterFunc(DataWriter::create)
                .overwrite()
                .rowSchema(SCHEMA)
                .withSpec(PartitionSpec.unpartitioned())
                .equalityFieldIds(table.schema().columns().stream().mapToInt(Types.NestedField::fieldId).toArray())
                .buildEqualityWriter();

        try (EqualityDeleteWriter<Record> writer = deleteWriter) {
            writer.deleteAll(records);
        }

        DeleteFile metadata = deleteWriter.toDeleteFile();
        Assert.assertEquals("Format should be Parquet", FileFormat.AVRO, metadata.format());
        Assert.assertEquals("Should be equality deletes", FileContent.EQUALITY_DELETES, metadata.content());
        Assert.assertEquals("Record count should be correct", records.size(), metadata.recordCount());
        Assert.assertEquals("Partition should be empty", 0, metadata.partition().size());
        Assert.assertNull("Key metadata should be null", metadata.keyMetadata());

        List<Record> deletedRecords;
        try (CloseableIterable<Record> reader = Avro.read(out.toInputFile())
                .project(SCHEMA)
                .createReaderFunc(DataReader::create)
                .build()) {
            deletedRecords = Lists.newArrayList(reader);
        }

//        try (CloseableIterable<Record> reader = Parquet.read(out.toInputFile())
//                .project(SCHEMA)
//                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema))
//                .build()) {
//            deletedRecords = Lists.newArrayList(reader);
//        }
        Assert.assertEquals("Deleted records should match expected", records, deletedRecords);
    }
}
