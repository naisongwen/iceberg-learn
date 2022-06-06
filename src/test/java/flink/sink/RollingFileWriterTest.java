//package flink.sink;
//
//import org.apache.flink.table.data.RowData;
//import org.apache.iceberg.Schema;
//import org.apache.iceberg.util.ArrayUtil;
//import org.junit.Assert;
//import org.junit.Test;
//import org.learn.datalake.flink.sink.FlinkFileWriterFactory;
//
//import java.io.IOException;
//import java.util.List;
//
//public class RollingFileWriterTest {
//    FlinkFileWriterFactory<RowData> newWriterFactory(Schema dataSchema, List<Integer> equalityFieldIds,
//                                                Schema equalityDeleteRowSchema,
//                                                Schema positionDeleteRowSchema) {
//        return FlinkFileWriterFactory.builderFor(table)
//                .dataSchema(table.schema())
//                .dataFileFormat(format())
//                .deleteFileFormat(format())
//                .equalityFieldIds(ArrayUtil.toIntArray(equalityFieldIds))
//                .equalityDeleteRowSchema(equalityDeleteRowSchema)
//                .positionDeleteRowSchema(positionDeleteRowSchema)
//                .build();
//    }
//
//    @Test
//    public void testRollingDataWriterNoRecords() throws IOException {
//        FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
//        RollingDataWriter<T> writer = new RollingDataWriter<>(
//                writerFactory, fileFactory, table.io(),
//                DEFAULT_FILE_SIZE, table.spec(), partition);
//
//        writer.close();
//        Assert.assertEquals("Must be no data files", 0, writer.result().dataFiles().size());
//
//        writer.close();
//        Assert.assertEquals("Must be no data files", 0, writer.result().dataFiles().size());
//    }
//}
