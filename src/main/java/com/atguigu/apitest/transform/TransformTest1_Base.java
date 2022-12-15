package com.atguigu.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;

public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据路径
        DataStream<String> dataStream= env.readTextFile("/Users/xiaomeichen/study/2022-study/tools_code/flinkStudy/src/main/resources/sensor.txt");
        //map输出
        DataStream<Integer> mapStream = dataStream.map(new MapFunction<String,Integer>() {

            @Override
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        //flatmap输出
        DataStream<String>  flatmapStream = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields=value.split(",");
                for(String field:fields){
                    out.collect(field);
                }

            }
        });

        //filter 筛选sensor_1开头的ID对应的数据
        DataStream<String> fileStream = dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("sensor_1");
            }
        });
        //打印输出

        mapStream.print("map");
        flatmapStream.print("flatMap");
        fileStream.print("filter");


        env.execute();
    }
}
