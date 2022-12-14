package com.atguigu.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据路径
        DataStream<String> dataStream= env.readTextFile("/Users/xiaomeichen/study/2022-study/tools_code/flinkStudy/src/main/resources/sensor.txt");

        //打印输出
        dataStream.print();

        env.execute();
    }
}
