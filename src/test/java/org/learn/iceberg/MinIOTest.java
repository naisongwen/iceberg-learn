package org.learn.iceberg;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.iceberg.types.Types.NestedField.required;

public class MinIOTest extends BaseTest{
  static final Schema SCHEMA = new Schema(
      required(3, "id", Types.IntegerType.get(), "unique ID"),
      required(4, "data", Types.StringType.get())
  );

  @Test
  public void testMinIOCreateDir() throws IOException {
    hiveConf.set(FS_DEFAULT_NAME_KEY,"s3a://bucket1");
    S3AFileSystem s3AFileSystem= (S3AFileSystem) S3AFileSystem.get(hiveConf);
    s3AFileSystem.create(new Path("s3a://bucket1/dlink-090f753804aa4ea299d0e45852ad5709-adf04df0/sddwwe_713"));
  }

  public void main(String[] args) throws IOException, URISyntaxException {
    File metastore_db = new File("metastore_db");
    try {
      if (metastore_db.exists()) {
        FileUtils.deleteDirectory(metastore_db);
      }
    } catch (Exception e) {
    }
    Configuration conf = new Configuration();
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.access.key", "admin123");
    conf.set("fs.s3a.secret.key", "admin123");
    conf.set("fs.s3a.endpoint", "http://10.201.0.212:34345");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    String warehouse = "s3a://tmp/iceberg/warehouse";

    S3AFileSystem s3AFileSystem= (S3AFileSystem) S3AFileSystem.get(new URI(warehouse), conf);
    s3AFileSystem.delete(new Path(warehouse),true);
    HadoopCatalog hadoopCatalog = new HadoopCatalog(conf,warehouse);
    TableIdentifier tbl1 = TableIdentifier.of("db", "tbl_1");
    // Table table = hadoopCatalog.createTable(tbl1, SCHEMA);
    //
    // Namespace nm = Namespace.of("db");
    // List<TableIdentifier> tableIdentifierList = hadoopCatalog.listTables(nm);
    // System.out.println(tableIdentifierList);

    HiveConf hiveConf = new HiveConf(conf, this.getClass());
    hiveConf.set("hive.metastore.schema.verification","false");
    hiveConf.set("datanucleus.schema.validateTables","false");
    hiveConf.set("datanucleus.schema.autoCreateTables","true");
    hiveConf.set("datanucleus.schema.autoCreateAll","true");
    String thriftUri="thrift://master:9083";
    TableIdentifier tbl2 = TableIdentifier.of("default", "tbl_4");
    Map<String, String> properties = new HashMap<>();
    properties.put("type", "iceberg");
    properties.put("property-version", "1");
    properties.put("catalog-type", "hive");
    properties.put("uri", thriftUri);
    properties.put("warehouse",warehouse);
    String HIVE_CATALOG = "iceberg_hive_catalog";
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUri);
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouse);
    hiveConf.set("fs.defaultFS", warehouse);
    CatalogLoader catalogLoader = CatalogLoader.hive(HIVE_CATALOG, hiveConf, properties);
    Catalog catalog = catalogLoader.loadCatalog();
    catalog.createTable(tbl2,SCHEMA,null,warehouse+"/default/tbl_4/",properties);
    Table loadTable=catalog.loadTable(tbl2);
    System.out.println(loadTable);
    //hiveCatalog.createTable(tbl2,SCHEMA,null,warehouse,null);
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
    conf.set("fs.s3a.endpoint.region", "cn-northwest-1");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.connection.ssl.enabled", "false");
    String warehouse = "s3a://dlink-public/";

    S3AFileSystem s3AFileSystem = (S3AFileSystem) S3AFileSystem.newInstance(new URI(warehouse), conf);
    String testPath = warehouse + "test";
    s3AFileSystem.create(new Path(testPath));
    RemoteIterator<LocatedFileStatus> remoteIterator = s3AFileSystem.listFiles(new Path(warehouse), true);
    while (remoteIterator.hasNext()) {
      LocatedFileStatus locatedFileStatus = remoteIterator.next();
      System.out.println(locatedFileStatus.getPath());
    }
  }
}
