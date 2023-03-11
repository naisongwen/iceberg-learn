package org.learn.iceberg;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Before;

public class BaseTest {
    protected HiveConf hiveConf;
    protected HiveMetaStoreClient hiveMetaStoreClient;
//        String hmsUri = "thrift://10.201.0.212:39083";
//    protected String hmsUri = "thrift://10.201.0.202:30470";
    String hmsUri = "thrift://localhost:9083";

    @Before
    public void init() throws MetaException {
        hiveConf = new HiveConf();
//        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "s3a://bucket/minio/");
        hiveConf.set("hive.metastore.client.capability.check", "false");
        hiveConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, hmsUri);
        hiveConf.set("fs.s3a.connection.ssl.enabled", "false");

        //dev
        hiveConf.set("fs.s3a.access.key", "admin1234");
        hiveConf.set("fs.s3a.secret.key", "admin1234");
        hiveConf.set("fs.s3a.endpoint", "http://10.201.0.212:32000");

        //test
        hiveConf.set("fs.s3a.access.key", "deepexi2021");
        hiveConf.set("fs.s3a.secret.key", "deepexi2021");
        hiveConf.set("fs.s3a.endpoint", "http://10.201.0.202:30977");


        //s3
        hiveConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hiveConf.set("fs.s3a.access.key", "AKIA4ZNT6QH3HFN6OYNO");
        hiveConf.set("fs.s3a.secret.key", "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");
        hiveConf.set("fs.s3a.endpoint", "http://s3.cn-northwest-1.amazonaws.com.cn");
        hiveConf.set("fs.s3a.endpoint.region","cn-northwest-1");
        hiveConf.set("fs.s3a.path.style.access", "true");
        hiveConf.set("fs.s3a.connection.ssl.enabled", "false");
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
    }
}
