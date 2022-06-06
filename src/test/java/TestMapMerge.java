import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.learn.datalake.iceberg.KafkaSinkIcebergExample;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.time.temporal.ChronoUnit.MILLIS;

public class TestMapMerge {
    public static void main(String[] args) throws Exception {

        Map<String, Object> statMetricsMap = Maps.newHashMap();
        statMetricsMap.put("successEventCount", 1L);
        statMetricsMap.put("failEventCount", 1L);
        statMetricsMap.put("statPeriodBegin", LocalDateTime.of(1970, Month.JANUARY,1,0,0));
        statMetricsMap.put("statPeriodEnd", LocalDateTime.of(1970, Month.JANUARY,1,0,0).plus(10,MILLIS));

        Map<String, String> fieldMetricsMap = Maps.newHashMap();
        fieldMetricsMap.put("valueType", "Long");
        fieldMetricsMap.put("valueCount", "1");
        fieldMetricsMap.put("maxValue", "2");
        fieldMetricsMap.put("minValue", "-2");
        statMetricsMap.put("field", fieldMetricsMap);


        Map<String, Object> statMetricsMap1 = Maps.newHashMap();
        statMetricsMap1.put("successEventCount", 2L);
        statMetricsMap1.put("failEventCount", 2L);
        statMetricsMap1.put("statPeriodBegin", LocalDateTime.of(1970, Month.JANUARY,1,0,0).plus(10,MILLIS));
        statMetricsMap1.put("statPeriodEnd", LocalDateTime.of(1970, Month.JANUARY,1,0,0).plus(20,MILLIS));
        Map<String, String> fieldMetricsMap1 = Maps.newHashMap();
        fieldMetricsMap1.put("valueType", "Long");
        fieldMetricsMap1.put("valueCount", "2");
        fieldMetricsMap1.put("maxValue", "3");
        fieldMetricsMap1.put("minValue", "-3");
        statMetricsMap1.put("field", fieldMetricsMap1);


        Stream<Map<String, Object>> mapStream = StreamSupport.stream(Arrays.asList(statMetricsMap).spliterator(), true);
        KafkaSinkIcebergExample.StatMapMergeOperator statMapMergeOperator=new KafkaSinkIcebergExample.StatMapMergeOperator();
        Optional<Map<String, Object>> statMap = mapStream.reduce(statMapMergeOperator);
        if (statMap.isPresent()) {
            //Map<String, Object> mergedMetricsMap = statMapMergeOperator.apply(statMetricsMap, statMetricsMap1);
            System.out.print(statMap.get());
        }
    }
}