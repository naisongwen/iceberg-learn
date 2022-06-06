package org.learn.datalake.flink.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;

import static org.apache.iceberg.TableProperties.*;

public class FlinkSink {

    private static final String ICEBERG_STREAM_WRITER_NAME = IcebergStreamWriter.class.getSimpleName();
    private static final String ICEBERG_FILES_COMMITTER_NAME = IcebergFilesCommitter.class.getSimpleName();

    private FlinkSink() {
    }

    /**
     * Initialize a {@link Builder} to export the data from generic input data stream into iceberg table. We use
     * {@link RowData} inside the sink connector, so users need to provide a mapper function and a
     * {@link TypeInformation} to convert those generic records to a RowData DataStream.
     *
     * @param input      the generic source input data stream.
     * @param mapper     function to convert the generic data to {@link RowData}
     * @param outputType to define the {@link TypeInformation} for the input data.
     * @param <T>        the data type of records.
     * @return {@link Builder} to connect the iceberg table.
     */
    public static <T> Builder builderFor(DataStream<T> input,MapFunction<T, RowData> mapper,TypeInformation<RowData> outputType) {
        return new Builder().forMapperOutputType(input, mapper, outputType);
    }

    /**
     * Initialize a {@link Builder} to export the data from input data stream with {@link Row}s into iceberg table. We use
     * {@link RowData} inside the sink connector, so users need to provide a {@link TableSchema} for builder to convert
     * those {@link Row}s to a {@link RowData} DataStream.
     *
     * @param input       the source input data stream with {@link Row}s.
     * @param tableSchema defines the {@link TypeInformation} for input data.
     * @return {@link Builder} to connect the iceberg table.
     */
    public static Builder forRow(DataStream<Row> input, TableSchema tableSchema) {
        RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
        DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();

        DataFormatConverters.RowConverter rowConverter = new DataFormatConverters.RowConverter(fieldDataTypes);
        return builderFor(input, rowConverter::toInternal, FlinkCompatibilityUtil.toTypeInfo(rowType))
                .tableSchema(tableSchema);
    }

    /**
     * Initialize a {@link Builder} to export the data from input data stream with {@link RowData}s into iceberg table.
     *
     * @param input the source input data stream with {@link RowData}s.
     * @return {@link Builder} to connect the iceberg table.
     */
    public static Builder forRowData(DataStream<GenericRowDataWithSchema> input) {
        return new Builder().forRowData(input);
    }

    public static class Builder {
        DataStream<GenericRowDataWithSchema> dataStream;
        private TableLoader tableLoader;
        private Table table;
        private TableSchema tableSchema;
        private boolean overwrite = false;
        private Integer writeParallelism = null;

        private Builder forRowData(DataStream<GenericRowDataWithSchema> dataStream) {
            this.dataStream=dataStream;
            return this;
        }

        public Builder table(Table newTable) {
            this.table = newTable;
            return this;
        }

        public Builder tableLoader(TableLoader newTableLoader) {
            this.tableLoader = newTableLoader;
            return this;
        }

        public Builder tableSchema(TableSchema newTableSchema) {
            this.tableSchema = newTableSchema;
            return this;
        }

        public Builder overwrite(boolean newOverwrite) {
            this.overwrite = newOverwrite;
            return this;
        }

        public Builder writeParallelism(int newWriteParallelism) {
            this.writeParallelism = newWriteParallelism;
            return this;
        }

        private Builder() {
        
        }

        private <T> Builder forMapperOutputType(DataStream<T> input,MapFunction<T, RowData> mapper,TypeInformation<RowData> outputType) {
            input.map(mapper, outputType);
            return this;
        }

        public DataStreamSink<GenericRowDataWithSchema> build() {
            if (table == null) {
                tableLoader.open();
                try (TableLoader loader = tableLoader) {
                    this.table = loader.loadTable();
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to load iceberg table from table loader: " + tableLoader, e);
                }
            }

            // Add parallel writers that append rows to files
            SingleOutputStreamOperator<WriteResult> writerStream = appendWriter(dataStream);

            // Add single-parallelism committer that commits files
            // after successful checkpoint or end of input
            SingleOutputStreamOperator<Void> committerStream = appendCommitter(writerStream);

            return appendDummySink(committerStream);
        }

        private SingleOutputStreamOperator<WriteResult> appendWriter(DataStream<GenericRowDataWithSchema> input) {
            IcebergStreamWriter<GenericRowDataWithSchema> streamWriter = createStreamWriter(tableLoader);

            int parallelism = writeParallelism == null ? input.getParallelism() : writeParallelism;
            SingleOutputStreamOperator<WriteResult> writerStream = input
                    .transform(ICEBERG_STREAM_WRITER_NAME, TypeInformation.of(WriteResult.class), streamWriter)
                    .setParallelism(parallelism);
            return writerStream;
        }

        private SingleOutputStreamOperator<Void> appendCommitter(SingleOutputStreamOperator<WriteResult> writerStream) {
            IcebergFilesCommitter filesCommitter = new IcebergFilesCommitter(tableLoader, overwrite);
            SingleOutputStreamOperator<Void> committerStream = writerStream
                    .transform(ICEBERG_FILES_COMMITTER_NAME, Types.VOID, filesCommitter)
                    .setParallelism(1)
                    .setMaxParallelism(1);
            return committerStream;
        }

        private DataStreamSink<GenericRowDataWithSchema> appendDummySink(SingleOutputStreamOperator<Void> committerStream) {
            DataStreamSink<GenericRowDataWithSchema> resultStream = committerStream
                    .addSink(new DiscardingSink())
                    .name(String.format("IcebergSink %s", this.table.name()))
                    .setParallelism(1);
            return resultStream;
        }

        static IcebergStreamWriter<GenericRowDataWithSchema> createStreamWriter(TableLoader tableLoader) {

            return new IcebergStreamWriter<>(tableLoader);
        }
    }


}
