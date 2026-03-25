package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkTest1_File2Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件读取数据路径
        DataStream<String> inputStream = env.readTextFile("/Users/xiaomeichen/study/2022-study/tools_code/flinkStudy/src/main/resources/sensor.txt");
        //lambda方式
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] sensor = line.split(",");

            return new SensorReading(sensor[0], new Long(sensor[1]), new Double(sensor[2]));
        });


        //定义Jedis连接配置：创建者模式
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.1.102")
                .setPort(6379)
                .build();

        //dataStream.addSink( new RedisSink<SensorReading>(config, new MyRedisMapper()) );
        dataStream.addSink(new RedisSink<SensorReading>(config, new MyRedisMapper()));
        env.execute();




    }
    public static class MyRedisMapper implements RedisMapper<SensorReading>{
        // 保存到 redis 的命令，存成哈希表
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_test");
        }
        public String getKeyFromData(SensorReading data) {
            return data.getId();
        }
        public String getValueFromData(SensorReading data) {
            return data.getTemperature().toString();
        }
    }


}
