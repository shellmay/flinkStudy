-- Kafka->Flink SQL->CSV 示例（基于窗口，避免 Update 变化）
CREATE TABLE kafka_source (
  `user` STRING,
  ts BIGINT,
  `value` DOUBLE,
  pt AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR pt AS pt - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'test-topic',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'g1',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE csv_sink (
  `user` STRING,
  window_end TIMESTAMP_LTZ(3),
  total DOUBLE,
  cnt BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///tmp/flink_csv_out',
  'format' = 'csv'
);

INSERT INTO csv_sink
SELECT `user`,
       TUMBLE_END(pt, INTERVAL '30' SECOND) AS window_end,
       SUM(`value`) AS total,
       COUNT(*) AS cnt
FROM kafka_source
GROUP BY `user`, TUMBLE(pt, INTERVAL '30' SECOND);
