package org.learn.datalake.catalog;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.learn.datalake.common.ExampleBase;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkIcebergExampleV2 extends ExampleBase {

    private static final String THRIFT_URI = "thrift://hw-node5:9083";
    //    private static final String THRIFT_URI = "thrift://localhost:50938";

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

    /**
     * /var/log/hadoop-hdfs/hadoop-cmf-hdfs-NAMENODE*.log.out
     *  org.apache.hadoop.security.ShellBasedUnixGroupsMapping: unable to return groups for user wns
     * PartialGroupNameException The user name 'wns' is not found. id: wns: no such user wns
     * //如果修改登录账户名称，设置环境变量：HADOOP_USER_NAME=hdfs
     * */

    public static void main(String[] args) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("property-version", "1");
        //properties.put("warehouse", parameterTool.get("warehouse"));
        properties.put(CatalogProperties.URI, THRIFT_URI);
        properties.put("catalog-type", "hive");
        // Set the 'hive-conf-dir' instead of 'warehouse'
        properties.put(FlinkCatalogFactory.HIVE_CONF_DIR, new File("src\\main\\resources").getAbsolutePath());

        FlinkIcebergExampleV2 flinkIcebergExampleV2=new FlinkIcebergExampleV2();
        List<Object[]> resultList=flinkIcebergExampleV2.sql("CREATE CATALOG test_catalog WITH %s",toWithClause(properties));
        System.out.println(resultList);
        flinkIcebergExampleV2.sql("USE CATALOG test_catalog");

        flinkIcebergExampleV2.sql("CREATE DATABASE IF not EXISTS test_db");
        flinkIcebergExampleV2.sql("USE test_db");
        flinkIcebergExampleV2.sql("CREATE TABLE IF not EXISTS test_table(c1 INT, c2 STRING)");
        flinkIcebergExampleV2.sql("INSERT INTO test_table SELECT 1, 'a'");
        flinkIcebergExampleV2.sql("DROP TABLE IF EXISTS test_table");
        flinkIcebergExampleV2.sql("DROP DATABASE IF EXISTS test_db");
    }
}