package org.learn.iceberg;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Before;

public class BaseTest {
    protected HiveConf hiveConf;
    protected HiveMetaStoreClient hiveMetaStoreClient;
    //    String hmsUri = "thrift://10.201.0.212:39083";
    protected String hmsUri = "thrift://10.201.0.202:30470";
//    String hmsUri = "thrift://localhost:9083";

    @Before
    public void init() throws MetaException {
        hiveConf = new HiveConf();
//        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "s3a://bucket/minio/");
//        hiveConf.set("metastore.catalog.default", "minio_217");
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
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
    }
}
