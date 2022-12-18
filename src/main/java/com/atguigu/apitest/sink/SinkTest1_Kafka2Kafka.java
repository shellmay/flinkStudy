package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.Properties;

public class SinkTest1_Kafka2Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件读取数据路径
        //DataStream<String> inputStream = env.readTextFile("/Users/xiaomeichen/study/2022-study/tools_code/flinkStudy/src/main/resources/sensor.txt");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.1.102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        //从kafka读取数据路径
        DataStream<String> inputStream= env.addSource(new FlinkKafkaConsumer011<String>("sensor",new SimpleStringSchema(),properties));
        //lambda方式
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] sensor = line.split(",");

            return new SensorReading(sensor[0], new Long(sensor[1]), new Double(sensor[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer011<String>("192.168.1.102:9092", "sinkTest", new SimpleStringSchema()));
        env.execute();




    }
}
