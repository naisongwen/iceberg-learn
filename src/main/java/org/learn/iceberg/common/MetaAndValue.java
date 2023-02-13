package org.learn.iceberg.common;

import com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Map;

public class MetaAndValue implements Serializable {
    private static final long serialVersionUID = 4813439951036021779L;
    private final TopicPartition tp;
    private final Long offset;
    private final String value;

    public MetaAndValue(TopicPartition tp, String value, Long offset) {
        this.tp = tp;
        this.value = value;
        this.offset = offset;
    }

    public String getValue() {
        return value;
    }

    public Map<String,Object> getMetaData(){
        Map<String,Object> meta= Maps.newHashMap();
        meta.put("__offset__",offset);
        meta.put("__topic__",tp.topic());
        meta.put("__partition__",tp.partition());
        return meta;
    }
}
