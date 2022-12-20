package com.atguigu.apitest.window;


import com.atguigu.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;

public class windowTest1_Timewindows2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


//        //从文件读取数据路径
//        DataStream<String> inputStream = env.readTextFile("/Users/xiaomeichen/study/2022-study/tools_code/flinkStudy/src/main/resources/sensor.txt");
//
//

        //socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("192.168.1.102", 7777);
        //lambda方式
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] sensor = line.split(",");

            return new SensorReading(sensor[0], new Long(sensor[1]), new Double(sensor[2]));
        });
        //开窗测试,全窗口函数
        DataStream<Integer> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Integer, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Integer> collector) throws Exception {
                        Integer count = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(count);
                    }
                });
        resultStream.print();
        env.execute();
    }
}
