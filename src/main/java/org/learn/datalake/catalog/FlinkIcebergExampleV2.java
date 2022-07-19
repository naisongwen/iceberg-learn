package org.learn.datalake.catalog;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.learn.datalake.common.ExampleBase;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkIcebergExampleV2 extends ExampleBase {

    static String toWithClause(Map<String, String> props) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");
        int propCount = 0;
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (propCount > 0) {
                builder.append(",");
            }
            builder.append("'").append(entry.getKey()).append("'").append("=")
                    .append("'").append(entry.getValue()).append("'");
            propCount++;
        }
        builder.append(")");
        return builder.toString();
    }


    public static void start() {
        String thriftUri="thrift://10.201.0.212:49153";
        String  warehouse="hdfs://10.201.0.82:9000/cjtest/catalogmanager/";
        String catalog="test_catalog_1";
        String table="test_table_1";
//        warehouse=new File("warehouse",table).getAbsolutePath();
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("property-version", "1");
        properties.put("warehouse",warehouse);
        properties.put(CatalogProperties.URI, thriftUri);
        properties.put("catalog-type", "hive");
        // Set the 'hive-conf-dir' instead of 'warehouse'
//        properties.put(FlinkCatalogFactory.HIVE_CONF_DIR, new File("src/main/resources").getAbsolutePath());

        sql(String.format("CREATE CATALOG %s WITH %s",catalog,toWithClause(properties)));
        sql(String.format("USE CATALOG %s",catalog));

        sql("CREATE DATABASE IF not EXISTS test_db");
        sql("USE test_db");
        sql(String.format("CREATE TABLE IF not EXISTS %s(c1 INT, c2 STRING) with('engine.hive.enabled'='true')",table));
        sql(String.format("INSERT INTO %s values(1, 'a')",table));
        List<Object[]> resultList=sql(String.format("select * from %s",table));
        System.out.println(resultList);

        sql(String.format("DROP TABLE IF EXISTS %s",table));
        sql("DROP DATABASE IF EXISTS test_db");

    }

    /**
     * /var/log/hadoop-hdfs/hadoop-cmf-hdfs-NAMENODE*.log.out
     *  org.apache.hadoop.security.ShellBasedUnixGroupsMapping: unable to return groups for user wns
     * PartialGroupNameException The user name 'wns' is not found. id: wns: no such user wns
     * //如果修改登录账户名称，设置环境变量：HADOOP_USER_NAME=hdfs
     * */

    public static void main(String[] args) {
        start();
    }
}
