package flink.source;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergQueryTest {
    private volatile TableEnvironment tEnv;

    @Test
    public void icebergQuery() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("property-version", "1");
        properties.put("catalog-type", "hive");
        properties.put("uri", "thrift://10.201.0.212:39083");
        properties.put("warehouse","s3a://test/");
        String HIVE_CATALOG = "iceberg_hive_catalog";
        // hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUri);
        HiveConf hiveConf = new HiveConf();
//        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouse);
        CatalogLoader catalogLoader = CatalogLoader.hive(HIVE_CATALOG, hiveConf, properties);
        TableIdentifier tableIdentifier = TableIdentifier.of("default", "tbl_14");
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader,tableIdentifier);

        getTableEnv().getConfig().getConfiguration().set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);

        List<Row> rowList=run(Maps.newHashMap(), "",tableIdentifier.name(),"*");
        System.out.println(rowList);
    }

    private TableEnvironment getTableEnv() {
        if (tEnv == null) {
            synchronized (this) {
                if (tEnv == null) {
                    this.tEnv = TableEnvironment.create(EnvironmentSettings
                            .newInstance()
                            .inBatchMode().build());
                }
            }
        }
        return tEnv;
    }

    protected List<Row> run(Map<String, String> sqlOptions, String sqlFilter,String tableName,
                            String... sqlSelectedFields) {
        String select = String.join(",", sqlSelectedFields);

        StringBuilder builder = new StringBuilder();
        sqlOptions.forEach((key, value) -> builder.append(optionToKv(key, value)).append(","));

        String optionStr = builder.toString();

        if (optionStr.endsWith(",")) {
            optionStr = optionStr.substring(0, optionStr.length() - 1);
        }

        if (!optionStr.isEmpty()) {
            optionStr = String.format("/*+ OPTIONS(%s)*/", optionStr);
        }

        return sql("select %s from %s %s %s", select,tableName, optionStr, sqlFilter);
    }


    private List<Row> sql(String query, Object... args) {
        TableResult tableResult = getTableEnv().executeSql(String.format(query, args));
        try (CloseableIterator<Row> iter = tableResult.collect()) {
            List<Row> results = Lists.newArrayList(iter);
            return results;
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect table result", e);
        }
    }

    private String optionToKv(String key, Object value) {
        return "'" + key + "'='" + value + "'";
    }
}
