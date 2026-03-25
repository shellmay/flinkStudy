package com.atguigu.wc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaToCsvStreaming {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "g1");

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
                "test-topic",
                new SimpleStringSchema(),
                props
        );

        DataStream<UserEvent> events = env.addSource(consumer)
                .map(new MapFunction<String, UserEvent>() {
                    @Override
                    public UserEvent map(String line) {
                        String clean = line.trim();
                        if (clean.startsWith("{") && clean.endsWith("}")) {
                            clean = clean.substring(1, clean.length() - 1);
                        }
                        String user = "";
                        long ts = 0L;
                        double value = 0.0;
                        for (String segment : clean.split(",")) {
                            String[] kv = segment.split(":");
                            if (kv.length < 2) continue;
                            String key = kv[0].replaceAll("\\\"", "").trim();
                            String val = kv[1].replaceAll("\\\"", "").trim();
                            if ("user".equals(key)) {
                                user = val;
                            } else if ("ts".equals(key)) {
                                ts = Long.parseLong(val);
                            } else if ("value".equals(key)) {
                                value = Double.parseDouble(val);
                            }
                        }
                        return new UserEvent(user, ts, value);
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(UserEvent element) {
                        return element.getTs();
                    }
                });

        DataStream<String> result = events
                .keyBy(new org.apache.flink.api.java.functions.KeySelector<UserEvent, String>() {
                    @Override
                    public String getKey(UserEvent value) {
                        return value.getUser();
                    }
                })
                .timeWindow(Time.seconds(30))
                .apply(new WindowFunction<UserEvent, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<UserEvent> input, Collector<String> out) {
                        double sum = 0;
                        long cnt = 0;
                        String user = "";
                        for (UserEvent e : input) {
                            user = e.getUser();
                            sum += e.getValue();
                            cnt++;
                        }
                        out.collect(user + "," + window.getEnd() + "," + sum + "," + cnt);
                    }
                });

        result.writeAsText("/tmp/flink_csv_out2/result.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("kafka-to-csv-streaming");
    }

    public static class UserEvent {
        private String user;
        private long ts;
        private double value;

        public UserEvent() { }

        public UserEvent(String user, long ts, double value) {
            this.user = user;
            this.ts = ts;
            this.value = value;
        }

        public String getUser() { return user; }
        public long getTs() { return ts; }
        public double getValue() { return value; }
    }
}
