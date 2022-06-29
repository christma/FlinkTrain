package cn.baise;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WCBatch {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSource<String> source = env.readTextFile("/Users/apple/IdeaProjects/FlinkTrain/flink-try/src/main/resources/data.txt");

        source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word:words){
                    collector.collect(Tuple2.of(word,1));
                }
            }
        }).groupBy(0).sum(1).print();


    }
}
