package org.learn.datalake.common;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.flink.FlinkTableOptions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public abstract class ExampleBase {

    protected static final Map<String, RowKind> ROW_KIND_MAP = org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.of(
            "+I", RowKind.INSERT,
            "-D", RowKind.DELETE,
            "-U", RowKind.UPDATE_BEFORE,
            "+U", RowKind.UPDATE_AFTER);

    protected static Row row(String rowKind, int id, String data) {
        RowKind kind = ROW_KIND_MAP.get(rowKind);
        if (kind == null) {
            throw new IllegalArgumentException("Unknown row kind: " + rowKind);
        }

        return Row.ofKind(kind, id, data);
    }

    protected static DataFormatConverters.RowConverter CONVERTER = new DataFormatConverters.RowConverter(
            SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());

    private volatile TableEnvironment tEnv = null;

    protected TableEnvironment getTableEnv() {
        if (tEnv == null) {
            synchronized (this) {
                if (tEnv == null) {
                    EnvironmentSettings settings = EnvironmentSettings
                            .newInstance()
                            .useBlinkPlanner()
                            .inBatchMode()
                            .build();

                    TableEnvironment env = TableEnvironment.create(settings);
                    env.getConfig().getConfiguration().set(FlinkTableOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
                    tEnv = env;
                }
            }
        }
        return tEnv;
    }

    protected static TableResult exec(TableEnvironment env, String query, Object... args) {
        return env.executeSql(String.format(query, args));
    }

    protected TableResult exec(String query, Object... args) {
        return exec(getTableEnv(), query, args);
    }

    protected List<Object[]> sql(String query, Object... args) {
        TableResult tableResult = exec(query, args);

        tableResult.getJobClient().ifPresent(c -> {
            try {
                c.getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        List<Object[]> results = Lists.newArrayList();
        try (CloseableIterator<Row> iter = tableResult.collect()) {
            while (iter.hasNext()) {
                Row row = iter.next();
                results.add(IntStream.range(0, row.getArity()).mapToObj(row::getField).toArray(Object[]::new));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return results;
    }
}
