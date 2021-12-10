package checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class StateBackEndApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(3, TimeUnit.SECONDS) // delay
        ));
        env.setStateBackend(new FsStateBackend("file:///Users/apple/IdeaProjects/FlinkTrain/logs/checkpoint"));

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);


        source.print();
        env.execute("StateBackEndApp");

    }
}
