package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;

public class Transform_Test2_RollingAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据路径
        DataStream<String> inputStream = env.readTextFile("/Users/xiaomeichen/study/2022-study/tools_code/flinkStudy/src/main/resources/sensor.txt");


        //转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String value) throws Exception {
//                String[] sensor=value.split(",");
//
//                return new SensorReading(sensor[0],new Long(sensor[1]),new Double(sensor[2]));
//            }
//        });


        //lambda方式
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] sensor = line.split(",");

            return new SensorReading(sensor[0], new Long(sensor[1]), new Double(sensor[2]));
        });

        //分组
        KeyedStream<SensorReading, Tuple> keyByStream = dataStream.keyBy("id");
        // KeyedStream<SensorReading, String> keyByStream1 = dataStream.keyBy(SensorReading::getId);

        //reduce聚合，取最大的温度值，以及当前最新的时间戳
        DataStream<SensorReading> reduceResoult = keyByStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTimestamp(), value2.getTemperature()));
            }
        });

        SingleOutputStreamOperator<SensorReading> reduce1Resoult1 = keyByStream.reduce((value1, value2) -> {
            return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
        });

        DataStream<SensorReading> temperature = keyByStream.max("temperature");
        //temperature.print();
        reduceResoult.print();
        reduce1Resoult1.print();
        env.execute();

    }
}
