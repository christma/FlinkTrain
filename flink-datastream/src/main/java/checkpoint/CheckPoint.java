package checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class CheckPoint {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(3, TimeUnit.SECONDS) // delay
        ));
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        if (value.contains("NN")) {
                            throw new RuntimeException("go go go go...");
                        } else {
                            return value.toLowerCase();
                        }
                    }
                }).flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] splits = value.split(",");
                        for (String split : splits) {
                            out.collect(split);
                        }
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                }).keyBy(key -> key.f0)
                .sum(1)
                .print();
        env.execute("CheckPoint");
    }
}
