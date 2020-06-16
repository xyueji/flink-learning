package com.xyueji.flink.core.model;

import java.util.Map;

/**
 * @author xiongzhigang
 * @date 2020-06-03 15:36
 * @description
 */
public class Metric {
    private String name;
    private long timestamp;
    private Map<String, Object> fields;
    private Map<String, String> tags;

    public Metric() {

    }

    public Metric(String name, long timestamp, Map<String, Object> fields, Map<String, String> tags) {
        this.name = name;
        this.timestamp = timestamp;
        this.fields = fields;
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "Metric{" +
                "name='" + name + '\'' +
                ", timestamp=" + timestamp +
                ", fields=" + fields +
                ", tags=" + tags +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
