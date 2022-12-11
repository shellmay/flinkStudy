package com.atguigu.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //String input="/Users/xiaomeichen/study/2022-study/tools_code/flinkStudy/src/main/resources/hello.txt";
        //DataStream<String> inputDataStream = env.readTextFile(input);


        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);

        //基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultstream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        resultstream.print();
        env.execute();
    }


}
