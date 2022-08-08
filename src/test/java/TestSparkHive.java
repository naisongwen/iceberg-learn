import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestSparkHive {
    @Test
    public void testHive() {
        SparkSession spark = SparkSession.builder().
                appName(TestSparkHive.class.getSimpleName()).
                master("local[*]").
                config(HiveConf.ConfVars.METASTOREURIS.varname,"thrift://10.201.0.202:49157").
                enableHiveSupport().
                getOrCreate();

        Catalog catalog = spark.catalog();
        spark.sessionState().catalogManager().v1SessionCatalog();
        spark.sessionState().catalogManager().v2SessionCatalog();
        List<Row> list= spark.sql("show databases").collectAsList();
        System.out.println(catalog);
    }
}
