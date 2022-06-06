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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.required;

public class HiveCatalogForMinIOExample {
  static final Schema SCHEMA = new Schema(
      required(3, "id", Types.IntegerType.get(), "unique ID"),
      required(4, "data", Types.StringType.get())
  );

  private static String getMetastorePath() {
    File metastoreDB = new File( "metastore_db");
    return metastoreDB.getPath();
  }

  private static void setupMetastoreDB(String dbURL) throws SQLException, IOException {
    Connection connection = DriverManager.getConnection(dbURL);
    ScriptRunner scriptRunner = new ScriptRunner(connection, true, true);

    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    InputStream inputStream = HiveCatalogForMinIOExample.class.getClassLoader().getResourceAsStream("hive-schema-3.1.0" +
        ".derby.sql");
    try (Reader reader = new InputStreamReader(inputStream)) {
      scriptRunner.runScript(reader);
    }
  }

  public static void main(String[] args) throws IOException, URISyntaxException, SQLException {
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

    setupMetastoreDB("jdbc:derby:" + getMetastorePath() + ";create=true");
    HiveConf hiveConf = new HiveConf(conf, HiveCatalogForMinIOExample.class);
    String thriftUri="thrift://master:9083";
    TableIdentifier tbl2 = TableIdentifier.of("default", "tbl_4");
    Map<String, String> properties = new HashMap<>();
    properties.put("type", "iceberg");
    properties.put("property-version", "1");
    properties.put("catalog-type", "hive");
    // properties.put("uri", thriftUri);
    properties.put("warehouse",warehouse);
    String HIVE_CATALOG = "iceberg_hive_catalog";
    // hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUri);
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouse);
    hiveConf.set("fs.defaultFS", warehouse);
    CatalogLoader catalogLoader = CatalogLoader.hive(HIVE_CATALOG, hiveConf, properties);
    Catalog catalog = catalogLoader.loadCatalog();
    catalog.createTable(tbl2,SCHEMA,null,warehouse+"/default/tbl_4/",properties);
    Table loadTable=catalog.loadTable(tbl2);
    System.out.println(loadTable);
  }
}
