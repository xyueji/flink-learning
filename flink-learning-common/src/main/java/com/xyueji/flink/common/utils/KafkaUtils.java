package com.xyueji.flink.common.utils;

import com.alibaba.fastjson.JSON;
import com.xyueji.flink.core.model.Metric;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

/**
 * @author xiongzhigang
 * @date 2020-06-03 15:42
 * @description
 */
public class KafkaUtils {
    private static final String broker_list = "office-computer:9092";
    private static final String topic = "metric";
    private static final Properties properties = new Properties();

    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

    static {
        properties.put("bootstrap.servers", broker_list);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public static void writeToKafka(Object metric) {
        KafkaProducer producer = new KafkaProducer<String, Object>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(metric));
        producer.send(record);

        log.info("发送数据：" + JSON.toJSONString(metric));
        producer.flush();
    }

    public static void main(String[] args) {
        while (true) {
            String name = "mem";
            long timestamp = System.currentTimeMillis();
            HashMap<String, Object> fields = new HashMap<String, Object>();
            fields.put("used_percent", 90d);
            fields.put("max", 27244873d);
            fields.put("used", 17244873d);
            fields.put("init", 27244873d);
            HashMap<String, String> tags = new HashMap<String, String>();
            tags.put("cluster", "cluster_test");
            tags.put("host_ip", "172.0.0.1");

            Metric metric = new Metric(name, timestamp, fields, tags);

            writeToKafka(metric);
        }
    }
}
