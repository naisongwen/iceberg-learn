import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_CAPABILITY_CHECK;

public class MinioTest {

    @Test
    public void testHive() throws TException {
        String catalogName = "linkhouse_927";
//                String thriftUri = "thrift://10.201.0.212:39083";
        String thriftUri = "thrift://10.201.0.84:9083";
        HiveConf hiveConf = new HiveConf();
//        hiveConf.set("metastore.catalog.default", catalogName);
        hiveConf.set(METASTORE_CAPABILITY_CHECK.varname,"false");
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUri);
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        hiveMetaStoreClient.getAllDatabases().forEach(System.out::println);

        org.apache.hadoop.hive.metastore.api.Table table = hiveMetaStoreClient.getTable("hive", "hexf07", "hive_test05");
        System.out.println(table);
    }

    @Test
    public void testMinIO() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.access.key", "admin1234");
        conf.set("fs.s3a.secret.key", "admin1234");
        conf.set("fs.s3a.endpoint", "http://10.201.0.212:32000");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        String warehouse = "s3a://faas-ethan/iceberg/warehouse";

        S3AFileSystem s3AFileSystem = (S3AFileSystem) S3AFileSystem.get(new URI(warehouse), conf);
        s3AFileSystem.delete(new Path(warehouse), true);
    }
}
