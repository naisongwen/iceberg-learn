package org.learn.iceberg.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iceberg.PartitionSpec.builderFor;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class HiveCatalogExample {
  protected static final String DB_NAME = "hive_db";
  static final String TABLE_NAME = "tbl";

  static final Schema schema = new Schema(Types.StructType.of(
      required(1, "id", Types.LongType.get())).fields());
  static final Schema altered = new Schema(Types.StructType.of(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.LongType.get())).fields());
  static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final PartitionSpec partitionSpec = builderFor(schema).identity("id").build();
  public static TemporaryFolder tempFolder = new TemporaryFolder();
  protected static HiveMetaStoreClient metastoreClient;
  protected static HiveConf hiveConf;
  protected static HiveCatalog hiveCatalog;

  private static org.apache.hadoop.hive.metastore.api.Table createHiveTable(String hiveTableName) throws IOException {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(serdeConstants.SERIALIZATION_CLASS, "org.apache.hadoop.hive.serde2.thrift.test.IntString");
    parameters.put(serdeConstants.SERIALIZATION_FORMAT, "org.apache.thrift.protocol.TBinaryProtocol");

    SerDeInfo serDeInfo = new SerDeInfo(null, "org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer", parameters);

    tempFolder.create();
    // StorageDescriptor has an empty list of fields - SerDe will report them.
    StorageDescriptor sd = new StorageDescriptor(Lists.newArrayList(), tempFolder.newFolder().getAbsolutePath(),
        "org.apache.hadoop.mapred.TextInputFormat", "org.apache.hadoop.mapred.TextOutputFormat",
        false, -1, serDeInfo, Lists.newArrayList(), Lists.newArrayList(), Maps.newHashMap());

    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        new org.apache.hadoop.hive.metastore.api.Table(hiveTableName, DB_NAME, "test_owner",
            0, 0, 0, sd, Lists.newArrayList(), Maps.newHashMap(),
            "viewOriginalText", "viewExpandedText", TableType.EXTERNAL_TABLE.name());
    return hiveTable;
  }

  //如果修改登录账户名称，设置环境变量：HADOOP_USER_NAME=hdfs
  public static void main(String[] args) throws Exception {
    //        String uri="thrift://localhost:" + 58883;
    String uri = "thrift://10.201.0.44:9083";
    hiveConf = new HiveConf(new Configuration(), HiveCatalogExample.class);
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, uri);
    File warehouse = new File("warehouse");
      if (warehouse.exists()) {
          FileUtils.deleteDirectory(warehouse);
      }
    String hiveLocalDir = warehouse.getAbsolutePath();
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:" + hiveLocalDir);
    hiveCatalog = new HiveCatalog();
    metastoreClient = new HiveMetaStoreClient(hiveConf);
    Database database = metastoreClient.getDatabase("default");

    String dbPath = new File(hiveLocalDir, DB_NAME + ".db").getPath();
    Database db = new Database(DB_NAME, "description", dbPath, new HashMap<>());
    // try {
    //   metastoreClient.dropDatabase(db.getName());
    // } catch (Exception e) {
    //
    // }
    // metastoreClient.createDatabase(db);

    // create a hive table
    String hiveTableName = "test_hive_table";
    org.apache.hadoop.hive.metastore.api.Table hiveTable = createHiveTable(hiveTableName);
    metastoreClient.createTable(hiveTable);
    Assert.assertTrue(hiveCatalog.tableExists(TABLE_IDENTIFIER));

    List<TableIdentifier> tableIdents = hiveCatalog.listTables(TABLE_IDENTIFIER.namespace());
    List<TableIdentifier> expectedIdents = tableIdents.stream()
        .filter(t -> t.namespace().level(0).equals(DB_NAME) && t.name().equals(TABLE_NAME))
        .collect(Collectors.toList());
    Assert.assertEquals(1, expectedIdents.size());
    metastoreClient.dropTable(DB_NAME, hiveTableName);

    Path tableLocation = new Path(hiveCatalog.createTable(TABLE_IDENTIFIER, schema, partitionSpec).location());
    Assert.assertTrue(hiveCatalog.tableExists(TABLE_IDENTIFIER));
    List<TableIdentifier> tableIdents1 = hiveCatalog.listTables(TABLE_IDENTIFIER.namespace());
    Assert.assertEquals("should only 1 iceberg table .", 1, tableIdents1.size());

    tableLocation.getFileSystem(hiveConf).delete(tableLocation, true);
    hiveCatalog.dropTable(TABLE_IDENTIFIER, false /* metadata only, location was already deleted */);
    metastoreClient.dropDatabase(db.getName());
  }
}
