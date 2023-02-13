package org.learn.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_CAPABILITY_CHECK;

public class HiveMetaStoreClientTest extends BaseTest{

    @Test
    public void testHive() throws TException {
        String catalogName = "linkhouse_927";
//                String thriftUri = "thrift://10.201.0.212:39083";
        String thriftUri = "thrift://10.201.0.84:9083";

        Database database=new Database("test_test_db_1","",null,null);
        //location determined by hive.metastore.warehouse.dir
//        hiveMetaStoreClient.createDatabase(database);

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
        String warehouse = "s3a://faas-ethan/iceberg/warehouse";

        S3AFileSystem s3AFileSystem = (S3AFileSystem) S3AFileSystem.get(new URI(warehouse), conf);
        s3AFileSystem.delete(new Path(warehouse), true);
    }

    @Test
    public void testS3() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.access.key", "AKIA4ZNT6QH3HFN6OYNO");
        conf.set("fs.s3a.secret.key", "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");
        conf.set("fs.s3a.endpoint", "http://s3.cn-northwest-1.amazonaws.com.cn");
        conf.set("fs.s3a.endpoint.region","cn-northwest-1");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        String warehouse = "s3a://dlink-public/";

        S3AFileSystem s3AFileSystem = (S3AFileSystem) S3AFileSystem.newInstance(new URI(warehouse),conf);
        String testPath= warehouse+"test";
        s3AFileSystem.create(new Path(testPath));
        RemoteIterator<LocatedFileStatus> remoteIterator= s3AFileSystem.listFiles(new Path(warehouse),true);
        while(remoteIterator.hasNext()){
            LocatedFileStatus locatedFileStatus=remoteIterator.next();
            System.out.println(locatedFileStatus.getPath());
        }
    }
}
