package org.learn.iceberg.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.iceberg.Schema;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ParseResult {

    boolean parseSuccess;
    Map<String, Object> keyValuesMap;
    String originContent;
    String errReason;
    Long processingTime;
    static Schema schema;

    protected ParseResult(Map<String, Object> keyValuesMap, Long processingTime) {
        this.parseSuccess = true;
        this.keyValuesMap = keyValuesMap;
        this.processingTime = processingTime;
    }


    protected ParseResult(String originContent, String errReason, Long processingTime) {
        this.parseSuccess = false;
        this.originContent = originContent;
        this.errReason = errReason;
        this.processingTime = processingTime;
    }

    public static ParseResult success(Map<String, Object> keyValuesMap, Long processingTime) {
        ParseResult parseResult = new ParseResult(keyValuesMap, processingTime);
        return parseResult;
    }

    public static ParseResult fail(String originContent, String errReason, Long processingTime) {
        ParseResult parseResult = new ParseResult(originContent, errReason, processingTime);
        return parseResult;
    }

    public static ParseResult parse(MetaAndValue metaAndValue) {
        Long processingTime = System.currentTimeMillis();
        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> map = Maps.newHashMap();
            map.putAll(metaAndValue.getMetaData());
            JsonNode node = mapper.readTree(metaAndValue.getValue());
            parseInternal("data", node, map);
            map.put("__processing_time__", processingTime);
            return ParseResult.success(map, processingTime);
        } catch (Exception e) {
            return ParseResult.fail(metaAndValue.toString(), e.getLocalizedMessage(), processingTime);
        }
    }

    public static Schema getSchema() {
        return schema;
    }

    public static void setSchema(Schema schema) {
        ParseResult.schema = schema;
    }

    private static void parseInternal(String key, JsonNode jsonNode, Map<String, Object> keyToValueMap) {
        String path = key;
        if (jsonNode.isValueNode()) {
            if (jsonNode.isInt())
                keyToValueMap.put(path, jsonNode.asInt());
            else if (jsonNode.isLong()||jsonNode.isBigInteger())
                keyToValueMap.put(path, jsonNode.asLong());
            //TODO:isBigDecimal 考虑精度
            else if (jsonNode.isFloat()||jsonNode.isBigDecimal()||jsonNode.isDouble())
                keyToValueMap.put(path, jsonNode.asDouble());
            else if (jsonNode.isBoolean())
                keyToValueMap.put(path, jsonNode.asBoolean());
            else
                keyToValueMap.put(path, jsonNode.toString());
        }

        if (jsonNode.isObject() || jsonNode.isPojo()) {
            Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                path = String.format("%s_%s", key, entry.getKey());
                parseInternal(path, entry.getValue(), keyToValueMap);
            }
        }

        if (jsonNode.isArray()) {
//            Iterator<JsonNode> it = jsonNode.iterator();
//            while (it.hasNext()) {
//                parseInternal(null,jsonNode);
//            }
            keyToValueMap.put(key, jsonNode.toString());
        }
    }

    public static void main(String[] args) throws IOException {
        final URL url = ParseResult.class.getClassLoader().getResource("dct-json-sample.json");
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        List<String> lines = Files.readAllLines(path);
        assert lines.size() > 0;
        for (String line : lines) {
            TopicPartition tp = new TopicPartition("test", 1);
            MetaAndValue metaAndValue = new MetaAndValue(tp, line, 0L);
            ParseResult.parse(metaAndValue);
        }
    }

    public Long getProcessingTime() {
        return processingTime;
    }

    public Map<String, Object> getKeyValuesMap() {
        return keyValuesMap;
    }

    public boolean isParseSuccess() {
        return parseSuccess;
    }
}
