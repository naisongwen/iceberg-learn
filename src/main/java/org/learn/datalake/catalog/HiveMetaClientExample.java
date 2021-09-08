package org.learn.datalake.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

public class HiveMetaClientExample {
    static final String DB_NAME = "hive_db";

    public static void main(String[] args) throws TException, FileNotFoundException {
        HiveConf hiveConf = new HiveConf();
        FileInputStream inputStream = new FileInputStream(new File("src/main/resources", "hive-site.xml"));
        Configuration newConf = new Configuration();
        newConf.addResource(inputStream);
        hiveConf.addResource(newConf);
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + 58883);
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        // 当前版本2.3.4与集群3.0版本不兼容，加入此设置
        hiveMetaStoreClient.setMetaConf("hive.metastore.client.capability.check", "false");
        Database database = new Database(DB_NAME, "", null, null);

        database = hiveMetaStoreClient.getDatabase(DB_NAME);
        // 根据数据库名称获取所有的表名
        List<String> tablesList = hiveMetaStoreClient.getAllTables(DB_NAME);
        // 由表名和数据库名称获取table对象(能获取列、表信息)
        for (String tableName : tablesList) {
            Table table = hiveMetaStoreClient.getTable(DB_NAME, tableName);
            List<FieldSchema> fieldSchemaList = table.getSd().getCols();
            for (FieldSchema fd : fieldSchemaList) {
                fd.getName();
            }
        }
        // 获取所有的列对象
        // 关闭当前连接
        hiveMetaStoreClient.close();
    }
}