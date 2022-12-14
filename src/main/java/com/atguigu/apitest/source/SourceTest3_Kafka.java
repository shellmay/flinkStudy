package com.atguigu.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.1.102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        //从kafka读取数据路径
        DataStream<String> dataStream= env.addSource(new FlinkKafkaConsumer011<String>("sensor",new SimpleStringSchema(),properties));

        //打印输出
        dataStream.print();

        env.execute();
    }
}
