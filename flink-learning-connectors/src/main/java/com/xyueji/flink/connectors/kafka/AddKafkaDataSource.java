package com.xyueji.flink.connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author xiongzhigang
 * @date 2020-06-03 16:15
 * @description
 */
public class AddKafkaDataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "office-computer:9092");
        props.put("zookeeper.connect", "office-computer:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); //value 反序列化

        String topic = "metric";

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props)).setParallelism(1);

        dataStreamSource.print();

        env.execute("Flink add kafka datasource");

    }
}
