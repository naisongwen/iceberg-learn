package org.learn.datalake.metadata;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.learn.datalake.common.TableTestBase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

//Reference SchemaEvolutionTest
public class SchemaEvolutionExample extends TableTestBase {

    public static void main(String[] args) throws Exception {
        File warehouse=new File("warehouse/test_schema");
        Table table = getTableOrCreate(warehouse,true);
        List<GenericRecord> recordList = mockSchemaData(table);
        DataFile dataFile = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-1.parquet"));
        table.newAppend()
                .appendFile(dataFile)
                .commit();

        table.updateSchema().addColumn("eventTime#1323", Types.StringType.get()).commit();
//        table.updateSchema().addColumn("eventTime1323", Types.TimestampType.withoutZone()).commit();
        recordList = mockChangedSchemaData(table);
        dataFile = writeParquetFile(table, recordList, new File(new File(warehouse.getAbsolutePath() + "/data/"), "data-2.parquet"));
        table.newAppend()
                .appendFile(dataFile)
                .commit();

        CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
        List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));

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


    static List<GenericRecord> mockSchemaData(Table table) {
        List<GenericRecord> records = new ArrayList<>();
        GenericRecord rec = GenericRecord.create(table.schema());
        rec.setField("id", 1);
        rec.setField("data", "aaa");
        records.add(rec);
        return records;
    }

    static List<GenericRecord> mockChangedSchemaData(Table table) {
        List<GenericRecord> records = new ArrayList<>();
        GenericRecord rec = GenericRecord.create(table.schema());
        rec.setField("id", 2);
        rec.setField("data", "bbb");
        //rec.setField("eventTime", LocalDateTime.parse("2020-10-29T10:01:00"));
        records.add(rec);
        return records;
    }
}
