package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transform_Test3_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据路径
        DataStream<String> inputStream = env.readTextFile("/Users/xiaomeichen/study/2022-study/tools_code/flinkStudy/src/main/resources/sensor.txt");

        //lambda方式
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] sensor = line.split(",");

            return new SensorReading(sensor[0], new Long(sensor[1]), new Double(sensor[2]));
        });

        //分组
        KeyedStream<SensorReading, Tuple> keyByStream = dataStream.keyBy("id");

        DataStream<SensorReading> resultStream = keyByStream.reduce(new MyMapFunction());




        env.execute();
    }
    public static class MyMapFunction extends RichMapFunction<SensorReading, Tuple2<Integer,String>> implements ReduceFunction<SensorReading> {

        @Override
        public Tuple2<Integer, String> map(SensorReading value) throws Exception {
            return new Tuple2<>(getIterationRuntimeContext().getIndexOfThisSubtask(),value.getId());
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("my map open");
        // 以下可以做一些初始化工作，例如建立一个和 HDFS 的连接
        }
        @Override
        public void close() throws Exception {
            System.out.println("my map close");

        }

        @Override
        public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
            return null;
        }

    }


    }
