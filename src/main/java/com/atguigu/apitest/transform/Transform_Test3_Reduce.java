package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.commons.math3.linear.SparseRealMatrix;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transform_Test3_Reduce {
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

        DataStream<SensorReading> resultStream = keyByStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });

        //lambda写法
        keyByStream.reduce((curData,newData)->
             new SensorReading(curData.getId(),newData.getTimestamp(),Math.max(curData.getTemperature(),newData.getTemperature()))
        );

        resultStream.print();
        env.execute();
    }
}
