package com.xyueji.flink.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author xiongzhigang
 * @date 2020-05-28 14:50
 * @description
 */
public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("SocketTextStreamWordCount USAGE: <hostname> <port>");
        }

        String hostname = args[0];
        Integer port = Integer.parseInt(args[1]);

        // 设置stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);

        // 计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter())
                .keyBy(0)
                .sum(1);

        sum.print();

        env.execute("SocketTextStreamWordCount");

    }

    /**
     * 分词，构建元组
     *
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) {
            String[] words = line.toLowerCase().split("\\W+");
            for (String word : words) {
                if (word.length() > 0) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
