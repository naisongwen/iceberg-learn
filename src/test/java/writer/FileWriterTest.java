//package writer;
//
//import org.apache.iceberg.DataFile;
//import org.apache.iceberg.FileFormat;
//import org.apache.iceberg.StructLike;
//import org.apache.iceberg.io.OutputFileFactory;
//import org.apache.iceberg.util.ArrayUtil;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import org.learn.datalake.common.WriterTestBase;
//import org.learn.datalake.flink.sink.FlinkFileWriterFactory;
//
//import java.util.List;
//
//public class FileWriterTest extends WriterTestBase {
//    private static final int TABLE_FORMAT_VERSION = 2;
//    private static final String PARTITION_VALUE = "aaa";
//
//    private final FileFormat fileFormat;
//    private final boolean partitioned;
//    private final List<T> dataRows;
//
//    private StructLike partition = null;
//    private OutputFileFactory fileFactory = null;
//
//
//    @Before
//    public void setupTable(){
//        fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
//
//    }
//
//    @Test
//    public void testWriteDataFile() {
//        FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
//
//        DataFile dataFile = writeData(writerFactory, dataRows, table.spec(), partition);
//
//        table.newRowDelta()
//                .addRows(dataFile)
//                .commit();
//
//        Assert.assertEquals("Records should match", toSet(dataRows), actualRowSet("*"));
//    }
//}
