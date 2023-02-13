package org.learn.iceberg;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class TypeConversionExample {
    DataType dataType= DataTypes.STRUCTURED(
            Tuple4.class,
            DataTypes.FIELD("valueCount", DataTypes.BIGINT()),
            DataTypes.FIELD("valueType", DataTypes.STRING()),
            DataTypes.FIELD("maxValue",DataTypes.DOUBLE()),
            DataTypes.FIELD("minValue",DataTypes.DOUBLE()));

    public static class StatMetrics {
        Long valueCount;
        String valueType;
        Double maxValue;
        Double minValue;

        public Long getValueCount() {
            return valueCount;
        }

        public void setValueCount(Long valueCount) {
            this.valueCount = valueCount;
        }

        public String getValueType() {
            return valueType;
        }

        public void setValueType(String valueType) {
            this.valueType = valueType;
        }

        public Double getMaxValue() {
            return maxValue;
        }

        public void setMaxValue(Double maxValue) {
            this.maxValue = maxValue;
        }

        public Double getMinValue() {
            return minValue;
        }

        public void setMinValue(Double minValue) {
            this.minValue = minValue;
        }
    }

    public static void main(String[] args) throws Exception {
        DataType dataType= DataTypes.STRUCTURED(
                StatMetrics.class,
                DataTypes.FIELD("valueCount", DataTypes.BIGINT()),
                DataTypes.FIELD("valueType", DataTypes.STRING()),
                DataTypes.FIELD("maxValue",DataTypes.DOUBLE()),
                DataTypes.FIELD("minValue",DataTypes.DOUBLE()));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
