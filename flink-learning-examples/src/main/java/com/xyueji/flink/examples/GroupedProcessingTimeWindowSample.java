package com.xyueji.flink.examples;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author xiongzhigang
 * @date 2020-06-04 13:33
 * @description
 */
public class GroupedProcessingTimeWindowSample {

    /**
     * 构造随机商品交易额
     *
     */
    public static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean isRunning = true;
        @Override
        public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep(2000);
                Tuple2<String, Integer> tuple2 = new Tuple2<>(String.valueOf((char)('A' + random.nextInt(10))), random.nextInt(100));
                sourceContext.collect(tuple2);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataSource source = new DataSource();

        DataStreamSource<Tuple2<String, Integer>> datasource = env.addSource(source);

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = datasource.keyBy(0);

        keyedStream.sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return "";
            }
        }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> fold(HashMap<String, Integer> stringIntegerHashMap, Tuple2<String, Integer> o) throws Exception {
                stringIntegerHashMap.put(o.f0, o.f1);
                return stringIntegerHashMap;
            }
        }).addSink(new SinkFunction<HashMap<String, Integer>>() {
            @Override
            public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
                System.out.println(value);
                System.out.println(value.values().stream().mapToInt(value1 -> value1).sum());
            }
        });

        env.execute("Flink datasource example");
    }
}
