package com.atguigu.wc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


//批处理WC
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //
        String input="/Users/xiaomeichen/study/2022-study/tools_code/flinkStudy/src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(input);


        //
        DataSet<Tuple2<String, Integer>>  wordCountDataSet =
                inputDataSet.flatMap(new MyFlatMapper())
                        .groupBy(0)
                        .sum(1);

        wordCountDataSet.print();
    }


    public static class  MyFlatMapper implements FlatMapFunction<String,Tuple2<String,Integer>>
    {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
