import com.google.common.collect.Iterables;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

public class TestQuery {
    public static void main(String[] args) throws Exception {
        Table distTable = new HadoopTables().load("C:\\Users\\wns\\Documents\\workplace\\datalake-learn\\target\\junit7742374570825396920\\junit960058545006666449\\test");
        distTable.refresh();
        try (
                CloseableIterable<Record> iterable = IcebergGenerics.read(distTable).build()) {
            String data = Iterables.toString(iterable);
            System.out.println(data);
        }
    }
}
