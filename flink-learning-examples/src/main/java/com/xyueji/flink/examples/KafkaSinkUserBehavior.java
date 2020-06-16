package com.xyueji.flink.examples;

import com.alibaba.fastjson.JSON;
import com.xyueji.flink.core.model.UserBehavior;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.File;
import java.net.URL;

/**
 * @author xiongzhigang
 * @date 2020-06-12 10:56
 * @description
 */
public class KafkaSinkUserBehavior {
    private static final String kafka_brokers = "office-computer:9092";
    private static final String topic = "user_behavior";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL url = KafkaSinkUserBehavior.class.getClassLoader().getResource("UserBehavior.csv");
        Path path = Path.fromLocalFile(new File(url.toURI()));
        PojoTypeInfo pojoTypeInfo = (PojoTypeInfo) TypeExtractor.createTypeInfo(UserBehavior.class);
        String[] fieldNames = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        PojoCsvInputFormat csvInputFormat = new PojoCsvInputFormat<>(path, pojoTypeInfo, fieldNames);

        DataStreamSource dataStreamSource = env.createInput(csvInputFormat, pojoTypeInfo);

        DataStreamSink produce = dataStreamSource.addSink(new FlinkKafkaProducer<UserBehavior>(kafka_brokers, topic, new SerializationSchema<UserBehavior>() {
            @Override
            public byte[] serialize(UserBehavior userBehavior) {
                return JSON.toJSONString(userBehavior).getBytes();
            }
        }));

        env.execute("flink to kafka");
    }
}
