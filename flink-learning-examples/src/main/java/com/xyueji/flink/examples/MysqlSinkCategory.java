package com.xyueji.flink.examples;

import com.alibaba.fastjson.JSON;
import com.xyueji.flink.connectors.mysql.SinkToMysql;
import com.xyueji.flink.core.model.UserBehavior;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author xiongzhigang
 * @date 2020-06-12 16:26
 * @description
 */
public class MysqlSinkCategory {
    private static final String topic = "user_behavior";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "office-computer:9092");
        props.put("group.id", "user-behavior-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); //value 反序列化

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> userBehavior = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props))
                .map(string -> {
                    return JSON.parseObject(string, UserBehavior.class);
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000;
                    }
                });

        userBehavior.timeWindowAll(Time.minutes(10)).apply(new AllWindowFunction<UserBehavior, List<UserBehavior>, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<UserBehavior> iterable, Collector<List<UserBehavior>> collector) throws Exception {
                ArrayList<UserBehavior> userBehaviors = Lists.newArrayList(iterable);

                if (userBehaviors.size() > 0) {
                    System.out.println("10min中收集到的数据条数为: " + userBehaviors.size());

                    collector.collect(userBehaviors);
                }
            }
        }).addSink(new SinkToMysql());

        env.execute("kafka to mysql");
    }
}
