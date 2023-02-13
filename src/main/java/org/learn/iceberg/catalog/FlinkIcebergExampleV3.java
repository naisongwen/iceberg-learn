package org.learn.iceberg.catalog;

import org.apache.iceberg.CatalogProperties;
import org.learn.iceberg.common.ExampleBase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkIcebergExampleV3 extends ExampleBase {

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
     * PartialGroupNameException The user name 'wns' is not found. id: wns: no such user wns
     * 如果修改登录账户名称，设置环境变量：HADOOP_USER_NAME=root
     * */

    public static void main(String[] args) {
        String thriftUri = "thrift://10.0.30.12:9083";
        String catalog="test_catalog_1";
        String table="test_table_2";
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("property-version", "1");
        String warehouse="/user/warehouse/hive/";
        properties.put("warehouse",warehouse);
        properties.put(CatalogProperties.URI, thriftUri);
        properties.put("catalog-type", "hive");

        FlinkIcebergExampleV3 flinkIcebergExampleV2=new FlinkIcebergExampleV3();
        flinkIcebergExampleV2.sql(String.format("CREATE CATALOG %s WITH %s",catalog,toWithClause(properties)));
        flinkIcebergExampleV2.sql(String.format("USE CATALOG %s",catalog));

        flinkIcebergExampleV2.sql("CREATE DATABASE IF not EXISTS test_db");
        flinkIcebergExampleV2.sql("USE test_db");
        flinkIcebergExampleV2.sql(String.format("CREATE TABLE %s (" +
                "                CD_ID BIGINT" +
                "                ) ",table));
        List<Object[]> resultList=flinkIcebergExampleV2.sql(String.format("select * from %s",table));
        System.out.println(resultList);

//        flinkIcebergExampleV2.sql(String.format("DROP TABLE IF EXISTS %s",table));
//        flinkIcebergExampleV2.sql("DROP DATABASE IF EXISTS test_db");
    }
}
