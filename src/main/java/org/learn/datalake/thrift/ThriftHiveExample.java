package org.learn.datalake.thrift;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.learn.datalake.common.ExampleBase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThriftHiveExample extends ExampleBase {

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
        properties.put(FlinkCatalogFactory.HIVE_CONF_DIR, "C:\\Users\\wns\\Documents\\workplace\\datalake-learn\\src\\main\\resources");

        ThriftHiveExample thriftHiveExample=new ThriftHiveExample();
        List<Object[]> resultList=thriftHiveExample.sql("CREATE CATALOG test_catalog WITH %s",toWithClause(properties));
        System.out.println(resultList);
        thriftHiveExample.sql("USE CATALOG test_catalog");

        thriftHiveExample.sql("CREATE DATABASE IF not EXISTS test_db");
        thriftHiveExample.sql("USE test_db");
        thriftHiveExample.sql("CREATE TABLE IF not EXISTS test_table(c1 INT, c2 STRING)");
        thriftHiveExample.sql("INSERT INTO test_table SELECT 1, 'a'");
        thriftHiveExample.sql("DROP TABLE IF EXISTS test_table");
        thriftHiveExample.sql("DROP DATABASE IF EXISTS test_db");
    }
}
