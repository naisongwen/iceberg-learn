/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.learn.datalake.common;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.*;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.*;
import org.apache.iceberg.relocated.com.google.common.io.Files;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.learn.datalake.common.SimpleDataUtil;
import org.learn.datalake.metadata.TestTables;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TableTestBase {

    public static Table getTableOrCreate(File warehouseDir, boolean cleanWarehouse) throws IOException {
        if (cleanWarehouse && warehouseDir.exists()) {
            FileUtils.cleanDirectory(warehouseDir);
            warehouseDir.mkdirs();
        }
        FileFormat format = FileFormat.valueOf("avro".toUpperCase(Locale.ENGLISH));
        Map<String, String> properties = com.google.common.collect.ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name(),"catalog-type", "hadoop","warehouse",warehouseDir.getAbsolutePath());
        String HADOOP_CATALOG = "iceberg_hadoop_catalog";
        CatalogLoader catalogLoader =
                CatalogLoader.hadoop(HADOOP_CATALOG, new Configuration(), properties);
        Catalog catalog = catalogLoader.loadCatalog();
        Table table;
        try {
             table = new HadoopTables().load(warehouseDir.getAbsolutePath());
        }catch (Exception e){
            table = SimpleDataUtil.createTable(warehouseDir.getAbsolutePath(), properties, false);
        }
        return table;
    }

    // Schema passed to create tables
    public static final Schema SCHEMA = new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get())
    );

    // Schema passed to create tables
    public static final Schema SCHEMA2 = new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()),
            required(3, "eventTime", Types.TimestampType.withoutZone())
    );

    // Partition spec used to create tables
    protected static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
            .bucket("data", 16)
            .build();

    static final DataFile FILE_A = DataFiles.builder(SPEC)
            .withPath("data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0") // easy way to set partition data for now
            .withRecordCount(1)
            .build();

    static final DataFile FILE_B = DataFiles.builder(SPEC)
            .withPath("data-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=1") // easy way to set partition data for now
            .withRecordCount(1)
            .build();

    static final DataFile FILE_A2 = DataFiles.builder(SPEC)
            .withPath("/path/to/data-a-2.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0") // easy way to set partition data for now
            .withRecordCount(1)
            .build();
    static final DeleteFile FILE_A_DELETES = FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0") // easy way to set partition data for now
            .withRecordCount(1)
            .build();
    // Equality delete files.
    static final DeleteFile FILE_A2_DELETES = FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(3)
            .withPath("/path/to/data-a2-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    static final DeleteFile FILE_B_DELETES = FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-b-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=1") // easy way to set partition data for now
            .withRecordCount(1)
            .build();
    static final DataFile FILE_C = DataFiles.builder(SPEC)
            .withPath("data-c.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=2") // easy way to set partition data for now
            .withRecordCount(1)
            .build();
    static final DataFile FILE_D = DataFiles.builder(SPEC)
            .withPath("/path/to/data-d.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=3") // easy way to set partition data for now
            .withRecordCount(1)
            .build();

    static final DataFile FILE_WITH_STATS = DataFiles.builder(SPEC)
            .withPath("/path/to/data-with-stats.parquet")
            .withMetrics(new Metrics(10L,
                    ImmutableMap.of(3, 100L, 4, 200L), // column sizes
                    ImmutableMap.of(3, 90L, 4, 180L), // value counts
                    ImmutableMap.of(3, 10L, 4, 20L), // null value counts
                    ImmutableMap.of(3, 0L, 4, 0L), // nan value counts
                    ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 1),
                            4, Conversions.toByteBuffer(Types.IntegerType.get(), 2)),  // lower bounds
                    ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 5),
                            4, Conversions.toByteBuffer(Types.IntegerType.get(), 10))  // upperbounds
            ))
            .withFileSizeInBytes(350)
            .build();

    static Iterator<Long> seqs(long... seqs) {
        return LongStream.of(seqs).iterator();
    }

    static Iterator<Long> ids(Long... ids) {
        return Iterators.forArray(ids);
    }

    static Iterator<DataFile> files(DataFile... files) {
        return Iterators.forArray(files);
    }

    static Iterator<DeleteFile> files(DeleteFile... files) {
        return Iterators.forArray(files);
    }

    protected static final int formatVersion=2;

    //private TestTables.TestTable table = null;
    File tableDir = null;
    File metadataDir = null;
    Table hadoopTab;

    Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "ts", Types.TimestampType.withoutZone())
    );


    @Before
    public void setupTable(File tableDir) throws Exception {
        this.tableDir = tableDir;

        this.metadataDir = new File(tableDir, "metadata");
        HadoopTables hadoopTables = new HadoopTables(new Configuration());
        hadoopTab = hadoopTables.create(schema, this.tableDir.getAbsolutePath());
        PartitionSpec spec = PartitionSpec.builderFor(schema).year("ts").bucket("id", 2).build();

    }

    @After
    public void cleanupTables() {
        TestTables.clearTables();
    }

    List<File> listManifestFiles() {
        return listManifestFiles(tableDir);
    }

    List<File> listManifestFiles(File tableDirToList) {
        return Lists.newArrayList(new File(tableDirToList, "metadata").listFiles((dir, name) ->
                !name.startsWith("snap") && Files.getFileExtension(name).equalsIgnoreCase("avro")));
    }

    protected TestTables.TestTable create(Schema schema, PartitionSpec spec) {
        return TestTables.create(tableDir, "test", schema, spec, formatVersion);
    }

    TestTables.TestTable load() {
        return TestTables.load(tableDir, "test");
    }

   static TestTables.LocalFileIO FILE_IO=new TestTables.LocalFileIO();
    public static ManifestFile writeManifest(Long snapshotId, Table table,File manifestFile, DataFile... files) throws IOException {
        OutputFile outputFile = FILE_IO.newOutputFile(manifestFile.getCanonicalPath());
        ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, table.spec(), outputFile, snapshotId);
        try {
            for (DataFile file : files) {
                writer.add(file);
            }
        } finally {
            writer.close();
        }
        return writer.toManifestFile();
    }

    ManifestFile writeDeleteManifest(int newFormatVersion, Long snapshotId, File manifestFile, DeleteFile... deleteFiles)
            throws IOException {
        OutputFile outputFile = org.apache.iceberg.Files
                .localOutput(FileFormat.AVRO.addExtension(manifestFile.getAbsolutePath()));
        ManifestWriter<DeleteFile> writer = ManifestFiles.writeDeleteManifest(
                newFormatVersion, SPEC, outputFile, snapshotId);
        try {
            for (DeleteFile deleteFile : deleteFiles) {
                writer.add(deleteFile);
            }
        } finally {
            writer.close();
        }
        return writer.toManifestFile();
    }

    ManifestFile writeManifestWithName(String name, File manifestFile,DataFile... files) throws IOException {
        Assert.assertTrue(manifestFile.delete());
        OutputFile outputFile = hadoopTab.io().newOutputFile(manifestFile.getCanonicalPath());
        ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, hadoopTab.spec(), outputFile, null);
        try {
            for (DataFile file : files) {
                writer.add(file);
            }
        } finally {
            writer.close();
        }

        return writer.toManifestFile();
    }

    void validateSnapshot(Snapshot old, Snapshot snap, long sequenceNumber, DataFile... newFiles) {
        validateSnapshot(old, snap, (Long) sequenceNumber, newFiles);
    }

    void validateTableFiles(Table tbl, DataFile... expectedFiles) {
        Set<CharSequence> expectedFilePaths = Sets.newHashSet();
        for (DataFile file : expectedFiles) {
            expectedFilePaths.add(file.path());
        }
        Set<CharSequence> actualFilePaths = Sets.newHashSet();
        for (FileScanTask task : tbl.newScan().planFiles()) {
            actualFilePaths.add(task.file().path());
        }
        Assert.assertEquals("Files should match", expectedFilePaths, actualFilePaths);
    }

    List<String> paths(DataFile... dataFiles) {
        List<String> paths = Lists.newArrayListWithExpectedSize(dataFiles.length);
        for (DataFile file : dataFiles) {
            paths.add(file.path().toString());
        }
        return paths;
    }

    protected static void printTableData(Table table){
        CloseableIterable<Record> iterable = IcebergGenerics.read(table).build();
        String data = com.google.common.collect.Iterables.toString(iterable);
        System.out.println("data in table "+table.name());
        System.out.println(data);
    }

    protected static void printManifest(Snapshot snapshot){
        List<ManifestFile> manifestFiles=snapshot.allManifests();
        for(ManifestFile m:manifestFiles) {
            System.out.println(m.path()+" owns datafiles as belows:");
            if(m.content()== ManifestContent.DATA) {
                ManifestReader<DataFile> reader = ManifestFiles.read(m, new TestTables.LocalFileIO());
                List<String> files = Streams.stream(reader)
                        .map(file -> file.path().toString())
                        .collect(Collectors.toList());
                for (CloseableIterator<DataFile> it = reader.iterator(); it.hasNext(); ) {
                    DataFile entry = it.next();
                    System.out.println(entry.path());
                }
            }else {
                ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(m, new TestTables.LocalFileIO(), null);
                List<String> files = Streams.stream(reader)
                        .map(file -> file.path().toString())
                        .collect(Collectors.toList());
                for (CloseableIterator<DeleteFile> it = reader.iterator(); it.hasNext(); ) {
                    DeleteFile entry = it.next();
                    System.out.println(entry.path());
                }
            }
        }
    }

    protected static DataFile writeParquetFile(Table table, List<GenericRecord> records, File parquetFile) throws IOException {
        FileAppender<GenericRecord> appender = Parquet.write(org.apache.iceberg.Files.localOutput(parquetFile))
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

    protected static DeleteFile writeDeleteFile(Table table, OutputFile out, StructLike partition,
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
}
