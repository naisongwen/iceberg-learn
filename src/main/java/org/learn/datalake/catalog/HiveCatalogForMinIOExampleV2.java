package org.learn.datalake.catalog;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.types.Types;
import org.learn.datalake.common.ExampleBase;

import static org.apache.iceberg.types.Types.NestedField.required;

public class HiveCatalogForMinIOExampleV2 extends ExampleBase {
  static final Schema SCHEMA = new Schema(
      required(3, "id", Types.IntegerType.get(), "unique ID"),
      required(4, "data", Types.StringType.get())
  );

  static final TableSchema FLINK_SCHEMA = TableSchema.builder()
      .field("id", DataTypes.INT())
      .field("data", DataTypes.STRING())
      .build();

  public static void main(String[] args)
      throws IOException, URISyntaxException, SQLException, TableAlreadyExistException {
    File metastore_db = new File("metastore_db");
    try {
      if (metastore_db.exists()) {
        FileUtils.deleteDirectory(metastore_db);
      }
    } catch (Exception e) {
    }
    Configuration conf = new Configuration();
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    // conf.set("fs.s3a.access.key", "admin123");
    // conf.set("fs.s3a.secret.key", "admin123");
    // conf.set("fs.s3a.endpoint", "http://10.201.0.212:34345");
    conf.set("fs.s3a.path.style.access", "true");
//    conf.set("fs.s3a.connection.ssl.enabled", "false");
    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    String query="k1=v1&fs.s3a.connection.ssl.enabled=false";
    String warehouse=String.format("%s?%s","s3a://admin123:admin123@10.201.0.212:34345/tmp/iceberg-default/warehouse",query);
    URI uri=new URI(warehouse);
    S3xLoginHelper.canonicalizeUri(uri,uri.getPort());
//    warehouse="s3a://tmp/iceberg/warehouse";
    S3AFileSystem s3AFileSystem= (S3AFileSystem) S3AFileSystem.get(uri, conf);
    String key=s3AFileSystem.pathToKey(new Path(warehouse));
    s3AFileSystem.delete(new Path(warehouse),true);

    HiveConf hiveConf = new HiveConf(conf, HiveCatalogForMinIOExampleV2.class);
    String thriftUri="thrift://localhost:62174";
    String tbl="tbl_1";
    TableIdentifier tbl2 = TableIdentifier.of("default",tbl);
    Map<String, String> properties = new HashMap<>();
    properties.put("type", "iceberg");
    properties.put("property-version", "1");
    properties.put("catalog-type", "hive");
    properties.put("uri", thriftUri);
    properties.put("warehouse",warehouse);
//    String location=String.format("s3a://admin123:admin123@10.201.0.212:34345/tmp/iceberg-default/warehouse/default/%s?%s",tbl,query);
//    properties.put("location",location);
    String HIVE_CATALOG = "iceberg_hive_catalog";
    // hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUri);
    // hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouse);
    hiveConf.set("fs.defaultFS", warehouse);
    CatalogLoader catalogLoader = CatalogLoader.hive(HIVE_CATALOG, hiveConf, properties);
    Catalog catalog = catalogLoader.loadCatalog();
    FlinkCatalog flinkCatalog=new FlinkCatalog("flinkCatalog","default",Namespace.empty(),catalogLoader,false);
    CatalogBaseTable baseTable=new CatalogTableImpl(FLINK_SCHEMA,properties,"");
    flinkCatalog.createTable(new ObjectPath("default",tbl),baseTable,false);
    //catalog.createTable(tbl2,SCHEMA,null,location,properties);
    Table loadTable=catalog.loadTable(tbl2);

    System.out.println(loadTable);
  }
}