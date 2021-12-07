package checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
                    throw new RuntimeException("gogogogo...");
                } else {
                    return value.toLowerCase();
                }
            }
        }).print();
        env.execute("CheckPoint");
    }
}
